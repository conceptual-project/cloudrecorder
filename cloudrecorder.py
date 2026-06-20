#!/usr/bin/env python3
"""
CloudRecorder — автоматическая запись звука с микрофона и выгрузка
записанных файлов в облачное хранилище (Яндекс.Диск / Google Drive) через rclone.

Единый скрипт: запись → очередь → загрузка в облако.

Оптимизировано для автономной работы 24/7 на устройствах с micro-SD:
  • асинхронное буферизованное логирование (QueueHandler/QueueListener);
  • кэширование результатов дорогих сетевых проверок;
  • минимизация операций записи на диск (убраны .recording маркеры);
  • O(N) очистка хранилища вместо O(N²);
  • прерываемые ожидания через shutdown_event.wait().
"""
import os
import sys
import time
import signal
import subprocess
import logging
import threading
import re
import json
from pathlib import Path
from datetime import datetime
from shutil import which, move, disk_usage
from queue import Queue
from logging.handlers import TimedRotatingFileHandler, QueueHandler, QueueListener
from typing import Literal, Optional, List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Pydantic для валидации конфига ---
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo, ValidationError
except ImportError:
    print("CRITICAL: Библиотека Pydantic не найдена. Установите её: pip install pydantic", file=sys.stderr)
    sys.exit(1)


# ========================================================
# Константы приложения
# ========================================================
MIN_FILE_SIZE_BYTES = 1024                  # Минимальный размер аудиофайла (байт) для сохранения
UPLOAD_QUEUE_CHUNK_SIZE = 10                # Макс. кол-во файлов, обрабатываемых за один проход очереди
SCHEDULE_CHECK_INTERVAL_SECONDS = 300       # Интервал логгирования статуса ожидания по расписанию
QUEUE_LOG_INTERVAL_SECONDS = 180            # Интервал логгирования кол-ва файлов в очереди
SCHEDULE_WAIT_SLEEP_SECONDS = 60            # Пауза в цикле, если мы вне расписания
PRODUCER_ERROR_SLEEP_SECONDS = 10           # Пауза после ошибки в главном цикле
CONSUMER_ERROR_SLEEP_SECONDS = 5            # Пауза после ошибки в потоке-потребителе
MIC_CHECK_TIMEOUT_SECONDS = 5               # Таймаут проверки микрофона (сек)
PING_PACKET_COUNT = 3                       # Кол-во пакетов ping для оценки скорости сети
LOG_RETENTION_DAYS = 14                     # Срок хранения логов (дней)
DISK_FREE_PERCENTAGE = 0.1                  # Минимальная доля свободного места на диске (10%)
DISK_FREE_MIN_BYTES = 1024 * 1024 * 1024    # Минимальный абсолютный порог свободного места (1 ГБ)
SHUTDOWN_TIMEOUT_SECONDS = 10               # Таймаут ожидания завершения потоков при остановке
SUBPROCESS_KILL_TIMEOUT_SECONDS = 5         # Таймаут ожидания завершения убитого подпроцесса
CONNECTIVITY_TIMEOUT_MULTIPLIER = 2         # Множитель таймаута для rclone about (рукопожатие авторизации)

# Кэширование дорогих сетевых/системных проверок (защита micro-SD от лишних процессов)
CONNECTIVITY_CACHE_TTL_SECONDS = 60         # TTL кэша check_internet_access
NETWORK_SPEED_CACHE_TTL_SECONDS = 300       # TTL кэша check_network_speed

# Поддерживаемые ffmpeg-кодировщики по форматам конфига
FFMPEG_ENCODERS: Dict[str, str] = {"opus": "libopus", "aac": "aac", "mp3": "libmp3lame"}


# ========================================================
# Модели конфигурации (Pydantic)
# ========================================================

class AudioConfig(BaseModel):
    split_time: int = Field(600, gt=0)
    sample_rate: int = Field(48000, gt=0)
    sample_format: str = "S24_3LE"
    mic: str = "default"
    format: Literal["opus", "aac", "mp3"] = "opus"
    bitrate: int = Field(64, gt=0)
    file_prefix: str = "redbox"
    ffmpeg_timeout_grace_period: int = Field(20, ge=5)


class CloudConfig(BaseModel):
    service: Literal["yandex", "google", "none"] = "yandex"
    delete_after_upload: bool = True
    retry_delay: int = Field(300, ge=0)
    max_retries: int = Field(15, ge=0)
    slow_network_retry_delay: int = Field(600, ge=0)
    slow_network_max_retries: int = Field(5, ge=0)
    network_speed_threshold: int = Field(100, gt=0)
    max_parallel_uploads: int = Field(1, gt=0)
    connectivity_timeout: int = Field(10, gt=0)
    connectivity_check_interval: int = Field(180, ge=0)
    ping_address: str = "77.88.8.8"


class StorageConfig(BaseModel):
    max_mb: int = Field(40960, gt=0)


class GoogleDriveConfig(BaseModel):
    remote: str = "google.drive"
    dir: str = "/Recordings"


class YandexDiskConfig(BaseModel):
    remote: str = "yandex.disk"
    dir: str = "/Recordings"


class ScheduleConfig(BaseModel):
    enabled: bool = False
    start_hour: int = Field(8, ge=0, lt=24)
    end_hour: int = Field(20, ge=0, lt=24)

    @field_validator('end_hour')
    @classmethod
    def start_must_be_before_end(cls, v: int, info: ValidationInfo) -> int:
        if info.data and 'start_hour' in info.data and info.data['start_hour'] >= v:
            raise ValueError('start_hour должен быть меньше end_hour')
        return v


class ConfigModel(BaseModel):
    output_dir: str = "/opt/cloudrecorder"
    log_file: str = "/opt/cloudrecorder/cloudrecorder.log"
    audio: AudioConfig = Field(default_factory=AudioConfig)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    google_drive: GoogleDriveConfig = Field(default_factory=GoogleDriveConfig)
    yandex_disk: YandexDiskConfig = Field(default_factory=YandexDiskConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)


# ========================================================
# Настройка логгера
# ========================================================
logger = logging.getLogger("cloudrecorder")


# ========================================================
# Главный класс
# ========================================================

class AudioRecorder:
    """Оркестрирует запись, обработку и выгрузку аудиофайлов."""

    def __init__(self, config_path: str = 'config.json'):
        # Состояние
        self.shutdown_event = threading.Event()
        self.in_schedule_mode = False
        self.config: ConfigModel = self._load_config(config_path)
        self._setup_logging()

        # Активные подпроцессы записи (для корректного завершения по сигналу)
        self._active_recording_procs: List[subprocess.Popen] = []
        self._recording_lock = threading.Lock()

        # Очередь для файлов, ожидающих обработки (продюсер-потребитель)
        self.work_queue: Queue = Queue()
        self.consumer_thread: Optional[threading.Thread] = None
        self.upload_thread: Optional[threading.Thread] = None
        self.upload_lock_path = os.path.join(self.config.output_dir, "upload.lock")

        # Кэш дорогих сетевых проверок (защита от частых spawn процессов на micro-SD)
        self._connectivity_cache: Tuple[float, bool] = (0.0, False)
        self._network_speed_cache: Tuple[float, str] = (0.0, "unknown")
        self._ffmpeg_encoders_checked: bool = False
        self._ffmpeg_encoders_available: bool = False

        # Предвычисляемые значения (конфиг статичен)
        self._file_extension: str = self.config.audio.format
        self._cloud_target: Optional[str] = self._compute_cloud_target()
        self._pending_dir: str = os.path.join(self.config.output_dir, "pending")
        self._file_pattern: str = (
            f"{self.config.audio.file_prefix}_*.{self._file_extension}"
        )

    # ----------------------------------------------------------
    # Конфигурация и логирование
    # ----------------------------------------------------------

    def _load_config(self, config_path: str) -> ConfigModel:
        """Загружает и валидирует конфигурацию из JSON-файла."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return ConfigModel(**data)
        except FileNotFoundError:
            print(f"CRITICAL: Файл конфигурации не найден: {config_path}", file=sys.stderr)
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"CRITICAL: Ошибка парсинга JSON в файле: {config_path}", file=sys.stderr)
            sys.exit(1)
        except ValidationError as e:
            print(f"CRITICAL: Ошибка валидации конфигурации в файле {config_path}:", file=sys.stderr)
            for error in e.errors():
                print(f"  - Поле: {'.'.join(map(str, error['loc']))}. Ошибка: {error['msg']}", file=sys.stderr)
            sys.exit(1)

    def _setup_logging(self) -> None:
        """Настраивает асинхронное буферизованное логирование.

        QueueHandler/QueueListener отделяют запись на диск от потока-источника:
        все log-сообщения попадают в in-memory очередь, а отдельный поток
        (QueueListener) записывает их в файл батчами. Это радикально снижает
        количество sync-операций на micro-SD при работе 24/7.
        """
        log_file = self.config.log_file
        os.makedirs(Path(log_file).parent, exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            log_file, when="midnight", interval=1, backupCount=LOG_RETENTION_DAYS, encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"
        ))

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"
        ))

        logger.setLevel(logging.INFO)

        # Асинхронная запись: QueueListener пишет в файл в отдельном потоке,
        # QueueHandler только кладёт сообщение в очередь (неблокирующе).
        self._log_queue: Queue = Queue()
        self._log_listener = QueueListener(
            self._log_queue, file_handler, stream_handler, respect_handler_level=True
        )
        self._log_listener.start()
        logger.addHandler(QueueHandler(self._log_queue))

    # ----------------------------------------------------------
    # Вспомогательные методы
    # ----------------------------------------------------------

    def _run_command(self, cmd_args: List[str], timeout: Optional[int] = None) -> Tuple[int, str, str]:
        """Безопасно выполняет команду без использования shell=True."""
        try:
            process = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding='utf-8',
                errors='replace'
            )
            if process.returncode != 0:
                logger.warning(
                    f"CMD failed: {' '.join(cmd_args)}\n"
                    f"Stderr: {process.stderr.strip()}"
                )
            return process.returncode, process.stdout, process.stderr
        except subprocess.TimeoutExpired:
            logger.error(f"Команда превысила таймаут: {' '.join(cmd_args)}")
            return -1, "", "timeout"
        except FileNotFoundError:
            logger.error(f"Команда не найдена: {cmd_args[0]}")
            return -1, "", f"not found: {cmd_args[0]}"
        except Exception as e:
            logger.error(f"Ошибка выполнения команды {' '.join(cmd_args)}: {e}")
            return -1, "", str(e)

    def _check_dependencies(self) -> None:
        """Проверяет наличие системных зависимостей: arecord, ffmpeg, rclone."""
        logger.info("Проверка зависимостей...")

        missing = []
        if not which("arecord"):
            missing.append("arecord (alsa-utils)")
        if not which("ffmpeg"):
            missing.append("ffmpeg")
        if self.config.cloud.service != "none" and not which("rclone"):
            missing.append("rclone")

        if missing:
            for m in missing:
                logger.critical(f"Зависимость не найдена: {m}")
            logger.critical("Установите недостающие пакеты и повторите запуск.")
            sys.exit(1)

        required_encoder = FFMPEG_ENCODERS[self.config.audio.format]
        if not self._ffmpeg_encoder_available(required_encoder):
            logger.critical(f"Кодировщик {required_encoder} для ffmpeg не найден.")
            sys.exit(1)

        logger.info("Все зависимости на месте.")

    def _ffmpeg_encoder_available(self, encoder_name: str) -> bool:
        """Точно проверяет наличие кодировщика в ffmpeg. Результат кэшируется."""
        if self._ffmpeg_encoders_checked:
            return self._ffmpeg_encoders_available
        code, out, _ = self._run_command(["ffmpeg", "-hide_banner", "-encoders"])
        self._ffmpeg_encoders_checked = True
        if code != 0:
            self._ffmpeg_encoders_available = False
            return False
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1] == encoder_name:
                self._ffmpeg_encoders_available = True
                return True
        self._ffmpeg_encoders_available = False
        return False

    def _compute_cloud_target(self) -> Optional[str]:
        """Вычисляет rclone-цель для текущего облачного сервиса (один раз)."""
        service = self.config.cloud.service
        if service == "google":
            return f"{self.config.google_drive.remote}:{self.config.google_drive.dir}"
        if service == "yandex":
            return f"{self.config.yandex_disk.remote}:{self.config.yandex_disk.dir}"
        return None

    # ----------------------------------------------------------
    # Проверка оборудования и сети (с кэшированием)
    # ----------------------------------------------------------

    def setup_mic(self) -> None:
        """Проверяет доступность микрофона короткой тестовой записью."""
        logger.info("Проверка микрофона...")
        audio_cfg = self.config.audio
        cmd = [
            'arecord', '-D', audio_cfg.mic,
            '-f', audio_cfg.sample_format,
            '-r', str(audio_cfg.sample_rate),
            '-c', '1',
            '-d', '1', '--quiet', '-t', 'wav'
        ]
        code, _, err = self._run_command(cmd, timeout=MIC_CHECK_TIMEOUT_SECONDS)
        if code != 0:
            logger.error(
                f"Ошибка доступа к микрофону {audio_cfg.mic}. "
                f"Убедитесь, что он подключён и настроен. Stderr: {err}"
            )
            sys.exit(1)
        logger.info(f"Микрофон готов: {audio_cfg.mic}")

    def check_internet_access(self) -> bool:
        """Проверяет доступность облачного remote через rclone about.

        Результат кэшируется на CONNECTIVITY_CACHE_TTL_SECONDS, чтобы не
        порождать rclone-процесс на каждую попытку загрузки (дорого для micro-SD
        и замедляет обработку очереди).
        """
        if self.config.cloud.service == "none":
            return True

        now = time.time()
        cached_at, cached_result = self._connectivity_cache
        if now - cached_at < CONNECTIVITY_CACHE_TTL_SECONDS:
            return cached_result

        if not self._cloud_target:
            self._connectivity_cache = (now, False)
            return False

        timeout = self.config.cloud.connectivity_timeout * CONNECTIVITY_TIMEOUT_MULTIPLIER
        cloud_remote = self._cloud_target.split(':')[0]
        code, _, _ = self._run_command(['rclone', 'about', f'{cloud_remote}:'], timeout=timeout)
        result = code == 0
        self._connectivity_cache = (now, result)
        return result

    def check_network_speed(self) -> str:
        """Оценивает скорость сети по среднему ping: 'fast', 'slow' или 'unknown'.

        Результат кэшируется на NETWORK_SPEED_CACHE_TTL_SECONDS (по умолчанию 5 мин),
        т.к. ping -c 3 занимает 3 секунды и не должен выполняться перед каждой загрузкой.
        """
        now = time.time()
        cached_at, cached_result = self._network_speed_cache
        if now - cached_at < NETWORK_SPEED_CACHE_TTL_SECONDS:
            return cached_result

        ping_address = self.config.cloud.ping_address
        code, out, err = self._run_command(['ping', '-c', str(PING_PACKET_COUNT), ping_address])
        if code != 0:
            logger.warning(f"Ping к {ping_address} не удался (код: {code}). Stderr: {err.strip()}.")
            self._network_speed_cache = (now, "unknown")
            return "unknown"
        try:
            match = re.search(r'rtt min/avg/max/mdev = .*?/([0-9.]+)/.*? ms', out)
            if match:
                avg = float(match.group(1))
                threshold = self.config.cloud.network_speed_threshold
                network_state = "slow" if avg > threshold else "fast"
                logger.info(f"Скорость сети определена как: {network_state} (avg ping: {avg:.2f}ms).")
                self._network_speed_cache = (now, network_state)
                return network_state
            logger.warning("Не удалось распарсить вывод ping. Скорость сети неизвестна.")
            self._network_speed_cache = (now, "unknown")
            return "unknown"
        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Ошибка при парсинге вывода ping: {e}. Скорость сети неизвестна.")
            self._network_speed_cache = (now, "unknown")
            return "unknown"

    # ----------------------------------------------------------
    # Восстановление после сбоя
    # ----------------------------------------------------------

    def recover_interrupted_files(self) -> None:
        """Восстанавливает незавершённые записи после аварийного завершения.

        Файлы в output_dir (не в pending) — незавершённые записи. Корректные
        по размеру ставятся в очередь, слишком маленькие удаляются.
        Файлы в pending/ уже готовы к выгрузке и не требуют обработки.
        """
        logger.info("Восстановление файлов после сбоя...")
        total_processed = 0
        total_corrupted = 0
        output_dir = self.config.output_dir

        for f in Path(output_dir).glob(self._file_pattern):
            try:
                if f.stat().st_size > MIN_FILE_SIZE_BYTES:
                    self.work_queue.put(str(f))
                    total_processed += 1
                    logger.info(f"Восстановлен и добавлен в очередь: {f.name}")
                else:
                    f.unlink()
                    total_corrupted += 1
                    logger.info(f"Удалён неполный файл: {f.name}")
            except Exception as e:
                logger.error(f"Ошибка при восстановлении файла {f.name}: {e}")

        # Маркеры .recording теперь не создаются (см. start_recording),
        # но на случай обновления со старой версии — очищаем их.
        for marker in Path(output_dir).glob("*.recording"):
            marker.unlink(missing_ok=True)

        logger.info(f"Восстановление завершено: обработано (+{total_processed}), удалено (-{total_corrupted})")

    # ----------------------------------------------------------
    # Запись
    # ----------------------------------------------------------

    def start_recording(self, file_path: str) -> bool:
        """Записывает один фрагмент аудио через arecord | ffmpeg.

        Маркер .recording НЕ создаётся на диске — для работы 24/7 на micro-SD
        каждая лишняя sync-запись изнашивает ячейки. Вместо этого активная
        запись отслеживается in-memory через _active_recording_procs, а
        восстановление после сбоя идёт по размеру файла.
        """
        audio_cfg = self.config.audio
        arecord_cmd = [
            'arecord', '-D', audio_cfg.mic,
            '-f', audio_cfg.sample_format,
            '-r', str(audio_cfg.sample_rate),
            '-c', '1',
            '-d', str(audio_cfg.split_time),
            '--quiet', '-'
        ]

        bitrate_arg = f'{audio_cfg.bitrate}k'
        encoder_cmd = {
            "opus": ['ffmpeg', '-y', '-i', '-', '-c:a', 'libopus', '-b:a', bitrate_arg,
                     '-application', 'voip', '-ac', '1', str(file_path),
                     '-hide_banner', '-loglevel', 'error'],
            "aac":  ['ffmpeg', '-y', '-i', '-', '-c:a', 'aac', '-b:a', bitrate_arg,
                     '-ac', '1', str(file_path),
                     '-hide_banner', '-loglevel', 'error'],
            "mp3":  ['ffmpeg', '-y', '-i', '-', '-c:a', 'libmp3lame', '-b:a', bitrate_arg,
                     '-ac', '1', str(file_path),
                     '-hide_banner', '-loglevel', 'error'],
        }.get(audio_cfg.format)

        arecord_proc = None
        ffmpeg_proc = None
        with self._recording_lock:
            try:
                # stderr=DEVNULL у arecord предотвращает дедлок при переполнении pipe-буфера.
                arecord_proc = subprocess.Popen(arecord_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                ffmpeg_proc = subprocess.Popen(
                    encoder_cmd,
                    stdin=arecord_proc.stdout,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE
                )
                if arecord_proc.stdout:
                    arecord_proc.stdout.close()

                self._active_recording_procs = [arecord_proc, ffmpeg_proc]

                _, stderr_data = ffmpeg_proc.communicate(
                    timeout=audio_cfg.split_time + audio_cfg.ffmpeg_timeout_grace_period
                )

                if ffmpeg_proc.returncode != 0:
                    # returncode == -15 (SIGTERM) ожидаем при graceful shutdown — не логируем как ошибку
                    if not self.shutdown_event.is_set():
                        logger.error(
                            f"Ошибка кодирования ffmpeg: "
                            f"{stderr_data.decode('utf-8', errors='ignore').strip()}"
                        )
                    return False

                logger.info(f"Запись завершена: {file_path}")
                return True

            except subprocess.TimeoutExpired:
                logger.error(f"Таймаут во время записи файла: {file_path}")
                return False
            except Exception as e:
                logger.error(f"Исключение во время записи: {e}")
                return False
            finally:
                # Гарантированно завершаем и собираем зомби-процессы
                for proc in (arecord_proc, ffmpeg_proc):
                    if proc is not None:
                        if proc.poll() is None:
                            proc.kill()
                        try:
                            proc.wait(timeout=SUBPROCESS_KILL_TIMEOUT_SECONDS)
                        except subprocess.TimeoutExpired:
                            pass
                self._active_recording_procs = []

    # ----------------------------------------------------------
    # Обработка и выгрузка
    # ----------------------------------------------------------

    def upload_to_cloud(self, file_path: str, network_status: Optional[str] = None) -> bool:
        """Загружает файл в облако с повторными попытками. Возвращает True при успехе."""
        cloud_cfg = self.config.cloud
        if cloud_cfg.service == "none":
            return True

        network = network_status if network_status else self.check_network_speed()
        max_retries = cloud_cfg.slow_network_max_retries if network == "slow" else cloud_cfg.max_retries
        retry_delay = cloud_cfg.slow_network_retry_delay if network == "slow" else cloud_cfg.retry_delay

        for attempt in range(1, max_retries + 1):
            if self.shutdown_event.is_set():
                return False

            # check_internet_access теперь кэшируется, поэтому вызов на каждой
            # попытке дешев; кэш инвалидируется при потере соединения автоматически.
            if not self.check_internet_access():
                logger.warning("Потеряно соединение с интернетом во время загрузки.")
                return False

            code, _, err = self._run_command(
                ['rclone', 'copy', str(file_path), self._cloud_target, '--quiet']
            )
            if code == 0:
                logger.info(f"Успешно загружено: {file_path}")
                if cloud_cfg.delete_after_upload:
                    Path(file_path).unlink(missing_ok=True)
                    logger.info(f"Файл удалён: {file_path}")
                return True

            logger.warning(
                f"Попытка загрузки {attempt}/{max_retries} не удалась: {file_path}. "
                f"Ошибка: {err.strip()}"
            )
            if attempt < max_retries:
                # shutdown_event.wait прерывается сигналом завершения, в отличие от time.sleep
                if self.shutdown_event.wait(retry_delay):
                    logger.info("Получен сигнал завершения во время ожидания повтора загрузки.")
                    return False

        logger.error(f"Загрузка провалена после {max_retries} попыток: {file_path}")
        return False

    def queue_for_upload(self, file_path: str) -> bool:
        """Перемещает готовый файл в директорию ожидания выгрузки (pending).

        Использует shutil.move, который на одной ФС сводится к os.rename (мгновенно,
        без копирования данных). pending/ всегда внутри output_dir, поэтому
        копирования не возникает.
        """
        pending_path = Path(self._pending_dir) / Path(file_path).name
        try:
            move(file_path, pending_path)
            logger.info(f"Файл добавлен в очередь: {pending_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка перемещения в очередь: {e}")
            return False

    def process_recorded_file(self, file_path: str) -> None:
        """Валидирует записанный файл и ставит его в очередь выгрузки.

        Один stat() вместо трёх системных вызовов (exists + stat + unlink/move):
        FileNotFoundError обрабатывается в едином try-блоке.
        """
        try:
            file_size = Path(file_path).stat().st_size
        except FileNotFoundError:
            logger.warning(f"Файл исчез до обработки: {file_path}")
            return

        if file_size < MIN_FILE_SIZE_BYTES:
            Path(file_path).unlink(missing_ok=True)
            logger.warning(f"Файл слишком маленький, удалён: {file_path}")
            return

        if self.config.cloud.service != "none":
            self.queue_for_upload(file_path)

    # ----------------------------------------------------------
    # Lock-файл очереди выгрузки
    # ----------------------------------------------------------

    def _is_process_running(self, pid: int) -> bool:
        """Проверяет, запущен ли процесс с указанным PID (Unix)."""
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            # Процесс существует, но принадлежит другому пользователю — считаем запущенным.
            return True
        return True

    def _acquire_upload_lock(self) -> bool:
        """Захватывает lock-файл для эксклюзивной обработки очереди выгрузки."""
        if os.path.exists(self.upload_lock_path):
            try:
                with open(self.upload_lock_path, 'r') as f:
                    pid = int(f.read().strip())
                if self._is_process_running(pid):
                    logger.info(f"Обработка очереди уже запущена процессом с PID {pid}, пропуск.")
                    return False
                logger.warning(f"Найден устаревший lock-файл от процесса PID {pid}. Перезаписываем.")
            except (ValueError, FileNotFoundError):
                logger.warning("Найден повреждённый lock-файл. Перезаписываем.")

        try:
            with open(self.upload_lock_path, 'w') as f:
                f.write(str(os.getpid()))
            return True
        except IOError as e:
            logger.error(f"Не удалось создать lock-файл: {e}")
            return False

    def _release_upload_lock(self) -> None:
        """Освобождает lock-файл очереди выгрузки.

        Безопасное удаление: проверяем, что lock принадлежит текущему процессу,
        иначе можно удалить lock параллельно работающего экземпляра.
        """
        try:
            if not os.path.exists(self.upload_lock_path):
                return
            with open(self.upload_lock_path, 'r') as f:
                pid_str = f.read().strip()
            if pid_str != str(os.getpid()):
                logger.warning(
                    f"Lock-файл принадлежит другому процессу (PID {pid_str}), не удаляем."
                )
                return
            os.unlink(self.upload_lock_path)
        except (IOError, ValueError) as e:
            logger.error(f"Не удалось удалить lock-файл: {e}")

    def _cleanup_stale_lock(self) -> None:
        """Удаляет устаревший lock-файл при старте, если процесс-владелец не активен."""
        if not os.path.exists(self.upload_lock_path):
            return
        try:
            with open(self.upload_lock_path, 'r') as f:
                pid_str = f.read().strip()
            pid = int(pid_str)
            if self._is_process_running(pid) and pid != os.getpid():
                logger.warning(
                    f"Найден активный lock-файл от процесса PID {pid}. "
                    f"Параллельный экземпляр? Lock не трогаем."
                )
                return
            logger.warning(f"Удаление устаревшего lock-файла от PID {pid}.")
            os.unlink(self.upload_lock_path)
        except (IOError, ValueError) as e:
            logger.warning(f"Удаление повреждённого lock-файла: {e}")
            try:
                os.unlink(self.upload_lock_path)
            except IOError:
                pass

    # ----------------------------------------------------------
    # Обработка очереди выгрузки
    # ----------------------------------------------------------

    def process_upload_queue(self) -> None:
        """Обрабатывает пакет файлов из pending-директории и загружает их в облако."""
        if self.config.cloud.service == "none":
            return

        if not self._acquire_upload_lock():
            return

        try:
            # Сразу после захвата lock проверяем shutdown — не начинаем работу,
            # если пришёл сигнал завершения.
            if self.shutdown_event.is_set():
                return

            if not self.check_internet_access():
                count = len(list(Path(self._pending_dir).glob(self._file_pattern)))
                logger.info(f"Нет интернета. Файлов в очереди: {count}")
                return

            self._cleanup_storage(force=False)

            files = sorted(
                Path(self._pending_dir).glob(self._file_pattern),
                key=lambda x: x.stat().st_mtime
            )[:UPLOAD_QUEUE_CHUNK_SIZE]
            if not files:
                return

            network = self.check_network_speed()
            max_workers = 1 if network == "slow" else self.config.cloud.max_parallel_uploads
            logger.info(f"Обработка очереди ({len(files)} файлов, сеть: {network}, потоков: {max_workers})")

            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='Uploader') as executor:
                if self.shutdown_event.is_set():
                    return

                future_to_file = {
                    executor.submit(self.upload_to_cloud, str(f), network): f for f in files
                }
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error(
                            f'Во время загрузки файла {file_path.name} произошло исключение: {exc}',
                            exc_info=True
                        )

                    if self.shutdown_event.is_set():
                        logger.info("Сигнал завершения получен во время обработки очереди. "
                                    "Отмена оставшихся задач.")
                        for fut in future_to_file:
                            fut.cancel()
                        break
        finally:
            self._release_upload_lock()

    # ----------------------------------------------------------
    # Расписание и очистка хранилища
    # ----------------------------------------------------------

    def in_recording_schedule(self) -> bool:
        """Проверяет, попадает ли текущее время в окно записи по расписанию."""
        schedule_cfg = self.config.schedule
        if not schedule_cfg.enabled:
            return True
        return schedule_cfg.start_hour <= datetime.now().hour < schedule_cfg.end_hour

    def _cleanup_storage(self, force: bool = True) -> None:
        """Удаляет самые старые файлы из pending, если превышен лимит хранилища.

        O(N) вместо O(N²): размер директории считается один раз, затем при
        удалении каждого файла вычитается его размер из накопленной суммы,
        а не пересканируется вся директория.
        """
        max_storage_mb = self.config.storage.max_mb
        max_storage_bytes = max_storage_mb * 1024 * 1024

        # Один проход по директории: (file_path, size) отсортированный по mtime
        entries = []
        total_bytes = 0
        try:
            for entry in os.scandir(self._pending_dir):
                if entry.is_file(follow_symlinks=False):
                    try:
                        st = entry.stat(follow_symlinks=False)
                        entries.append((entry.path, st.st_size, st.st_mtime))
                        total_bytes += st.st_size
                    except OSError as e:
                        logger.error(f"Не удалось получить stat для {entry.path}: {e}")
        except OSError as e:
            logger.error(f"Ошибка сканирования pending-директории: {e}")
            return

        if total_bytes <= max_storage_bytes:
            return

        if force:
            logger.warning(
                f"Превышен лимит хранилища ({total_bytes // (1024 * 1024)}MB > "
                f"{max_storage_mb}MB), принудительная очистка..."
            )

        # Сортируем по mtime (старые первыми) и удаляем до достижения лимита
        entries.sort(key=lambda x: x[2])
        for path, size, _ in entries:
            if total_bytes <= max_storage_bytes:
                break
            try:
                os.unlink(path)
                total_bytes -= size
                logger.info(f"Удалён старый файл из очереди: {Path(path).name}")
            except OSError as e:
                logger.error(f"Не удалось удалить файл {path}: {e}")

    def log_queue_status(self) -> None:
        """Логирует количество файлов, ожидающих выгрузки."""
        count = len(list(Path(self._pending_dir).glob(self._file_pattern)))
        logger.info(f"Файлов в очереди на загрузку: {count}")

    # ----------------------------------------------------------
    # Завершение работы
    # ----------------------------------------------------------

    def graceful_shutdown(self, signum: int, frame) -> None:
        """Обработчик сигналов SIGINT/SIGTERM: инициирует корректную остановку."""
        logger.info(f"Получен сигнал {signum}, завершение работы...")
        self.shutdown_event.set()

        # Завершаем активные подпроцессы записи, чтобы producer-loop разблокировался
        with self._recording_lock:
            for proc in self._active_recording_procs:
                if proc is not None and proc.poll() is None:
                    proc.terminate()

        # Даём текущей задаче выгрузки завершиться (с таймаутом)
        if self.upload_thread and self.upload_thread.is_alive():
            logger.info("Ожидание завершения текущей задачи загрузки...")
            self.upload_thread.join(timeout=SHUTDOWN_TIMEOUT_SECONDS)

        logger.info("Сигнал обработан, ожидание естественного завершения циклов.")

    # ----------------------------------------------------------
    # Главные циклы
    # ----------------------------------------------------------

    def _producer_loop(self) -> None:
        """Главный цикл: запись фрагментов и запуск обработки очереди."""
        last_connectivity_check = 0
        last_queue_log = 0
        last_schedule_log = 0

        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()

                # --- Проверка расписания ---
                if self.config.schedule.enabled:
                    if self.in_recording_schedule():
                        if not self.in_schedule_mode:
                            logger.info(
                                f"Начало записи по расписанию "
                                f"({self.config.schedule.start_hour}:00–"
                                f"{self.config.schedule.end_hour}:00)"
                            )
                            self.in_schedule_mode = True
                    else:
                        if self.in_schedule_mode or \
                                (current_time - last_schedule_log > SCHEDULE_CHECK_INTERVAL_SECONDS):
                            logger.info(
                                f"Вне расписания записи "
                                f"({self.config.schedule.start_hour}:00–"
                                f"{self.config.schedule.end_hour}:00). Ожидание..."
                            )
                            self.in_schedule_mode = False
                            last_schedule_log = current_time
                        # shutdown_event.wait прерывается сигналом, в отличие от time.sleep
                        if self.shutdown_event.wait(SCHEDULE_WAIT_SLEEP_SECONDS):
                            break
                        continue

                # --- Периодический запуск обработчика очереди выгрузки ---
                if current_time - last_connectivity_check >= self.config.cloud.connectivity_check_interval:
                    last_connectivity_check = current_time
                    if not self.upload_thread or not self.upload_thread.is_alive():
                        self.upload_thread = threading.Thread(
                            target=self.process_upload_queue,
                            name="QueueProcessor",
                            daemon=True
                        )
                        self.upload_thread.start()

                self._cleanup_storage(force=True)

                # --- Логирование размера очереди ---
                if current_time - last_queue_log >= QUEUE_LOG_INTERVAL_SECONDS:
                    self.log_queue_status()
                    last_queue_log = current_time

                # --- Проверка свободного места на диске ---
                total_disk_space = disk_usage(self.config.output_dir)
                # Порог: меньшее из DISK_FREE_PERCENTAGE от total и DISK_FREE_MIN_BYTES
                free_threshold_bytes = min(
                    total_disk_space.total * DISK_FREE_PERCENTAGE,
                    DISK_FREE_MIN_BYTES
                )
                if total_disk_space.free < free_threshold_bytes:
                    logger.critical(
                        f"Критически мало свободного места на диске "
                        f"({total_disk_space.free / (1024 * 1024):.2f} MB свободно). "
                        f"Запись приостановлена."
                    )
                    if self.shutdown_event.wait(SCHEDULE_WAIT_SLEEP_SECONDS):
                        break
                    continue

                # --- Запись фрагмента ---
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_path = os.path.join(
                    self.config.output_dir,
                    f"{self.config.audio.file_prefix}_{ts}.{self._file_extension}"
                )

                logger.info(f"Начало записи: {Path(file_path).name}")
                if self.start_recording(file_path):
                    self.work_queue.put(file_path)
                else:
                    if not self.shutdown_event.is_set():
                        logger.error(f"Не удалось выполнить запись для {Path(file_path).name}")

                if self.shutdown_event.is_set():
                    break

            except Exception as e:
                logger.critical(
                    f"Критическая ошибка в главном цикле (продюсер). Ошибка: {e}",
                    exc_info=True
                )
                if self.shutdown_event.wait(PRODUCER_ERROR_SLEEP_SECONDS):
                    break

    def _consumer_loop(self) -> None:
        """Поток-потребитель: берёт файлы из очереди и обрабатывает их.

        Корректная обработка task_done: вызывается ровно один раз в finally
        для каждого элемента очереди. Сигнал завершения (None) обрабатывается
        отдельно и тоже с одним task_done.
        """
        logger.info("Потребитель запущен, ожидает файлы для обработки.")
        while True:
            file_path = self.work_queue.get()
            is_shutdown_signal = (file_path is None)

            try:
                if is_shutdown_signal:
                    logger.info("Получен сигнал завершения, потребитель останавливается.")
                    break

                logger.info(f"Потребитель получил файл: {Path(file_path).name}")
                self.process_recorded_file(file_path)
            except Exception as e:
                logger.critical(
                    f"Критическая ошибка в потоке потребителя при обработке файла "
                    f"'{file_path}'. Ошибка: {e}",
                    exc_info=True
                )
                if self.shutdown_event.wait(CONSUMER_ERROR_SLEEP_SECONDS):
                    break
            finally:
                # Ровно один task_done на каждый get() — исправление double-task_done бага
                self.work_queue.task_done()

        logger.info("Потребитель завершил работу.")

    # ----------------------------------------------------------
    # Точка входа
    # ----------------------------------------------------------

    def run(self) -> None:
        """Инициализирует окружение и запускает главные циклы."""
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        # Безопасная очистка устаревшего lock (с проверкой PID), а не безусловное удаление
        self._cleanup_stale_lock()

        os.makedirs(self._pending_dir, exist_ok=True)

        if not os.access(self.config.output_dir, os.W_OK) or not os.access(self._pending_dir, os.W_OK):
            logger.critical(
                f"Нет прав на запись в директории {self.config.output_dir} или {self._pending_dir}"
            )
            sys.exit(1)

        self._check_dependencies()
        self.setup_mic()
        self.recover_interrupted_files()

        logger.info(
            f"▶ Запуск записи в формате {self.config.audio.format} "
            f"с выгрузкой на {self._cloud_name()}"
        )

        self.consumer_thread = threading.Thread(target=self._consumer_loop, name="FileConsumer")
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

        try:
            self._producer_loop()
        finally:
            logger.info("Продюсер остановлен. Отправка сигнала завершения потребителю.")
            self.work_queue.put(None)
            self.consumer_thread.join(timeout=SHUTDOWN_TIMEOUT_SECONDS)

            # Финальное ожидание потока выгрузки
            if self.upload_thread and self.upload_thread.is_alive():
                self.upload_thread.join(timeout=SHUTDOWN_TIMEOUT_SECONDS)

            self._release_upload_lock()

            # Останавливаем асинхронный log-listener, сбрасывая остатки буфера на диск
            self._log_listener.stop()

            logger.info("Все потоки корректно завершены.")

    def _cloud_name(self) -> str:
        """Человекочитаемое имя облачного сервиса (без кэширования, вызов редкий)."""
        return {
            "google": "Google Drive",
            "yandex": "Яндекс.Диск",
            "none": "локальное хранилище",
        }.get(self.config.cloud.service, "неизвестно")


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'config.json'
    recorder = AudioRecorder(config_path=config_file)
    try:
        recorder.run()
    except Exception as e:
        logger.critical(f"Критическая ошибка в главном потоке: {e}", exc_info=True)
        sys.exit(1)
