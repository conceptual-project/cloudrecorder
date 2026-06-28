# CloudRecorder

**Automatic microphone audio recording with cloud upload of recorded files.**

CloudRecorder is a standalone Python application designed for long-term autonomous audio recording on Raspberry Pi or any other SBC (Single-Board Computer). The program records audio in fragments of a specified duration, queues the finished files, and asynchronously uploads them to the cloud (Yandex Disk or Google Drive) via `rclone`.

The project is hardened for **24/7 unattended operation on a Raspberry Pi Zero 2 W** with an unstable Internet connection and unreliable power: every external subprocess has bounded execution time, the upload queue keeps draining whenever the network is available (independently of the recording schedule), and a single-process `flock` guarantees no duplicate upload runs.

---

## Features

- **Audio recording** from a microphone via `arecord` (ALSA) with on-the-fly `ffmpeg` transcoding.
- **Encoding formats** to choose from: `opus`, `aac`, `mp3` (mono, configurable bitrate).
- **Fragmentation** — recording is split into files of fixed duration (10 minutes by default).
- **Cloud upload** via `rclone` (Yandex Disk, Google Drive, or disabled — local storage only).
- **Bounded rclone execution** — every `rclone` invocation is launched with `--contimeout`, `--timeout` and `--low-level-retries`, so a single hung upload can never block the queue processor indefinitely on a flaky network.
- **Network speed adaptation**: measures average ping, with separate retry limits and delays for slow connections.
- **Parallel upload** of multiple files at once (`ThreadPoolExecutor`) on fast networks. On `unknown`/`slow` networks it is forced back to a single thread — safer for the Pi Zero's 512 MB RAM.
- **Schedule-based operation** — record only within a specified time window (e.g., 08:00–20:00). **The upload queue and storage cleanup keep running outside the recording window**, so a backlog accumulated during the day is uploaded as soon as the network comes back — even at night.
- **Free space monitoring**: when the storage limit is exceeded, the oldest files are automatically deleted from the queue. The default parameters assume the system has ~25 GB of free space.
- **Crash recovery** — on restart, unfinished recordings and files left in the upload queue are handled correctly.
- **Log rotation** — daily, retaining logs for the last 14 days. Logging is asynchronous (`QueueHandler`/`QueueListener`) to minimise sync writes on micro-SD.
- **Graceful shutdown** on `SIGINT` / `SIGTERM` signals, waiting for in-flight tasks to complete. The signal handler is intentionally minimal (no `join()` inside it) so the main thread is never blocked.
- **Runs as a systemd service** (non-root, `Restart=always`, `KillMode=control-group`).

---

## Architecture

The application follows a **producer–consumer** pattern with three threads:

```
┌──────────────────────────────────────────────────────────────────┐
│  Main thread (Producer)                                          │
│  ─────────────────────────────                                   │
│  • ALWAYS: launches the upload queue processor (on interval)     │
│  • ALWAYS: runs storage cleanup + logs queue size                │
│  • Checks schedule + free disk space (gates ONLY recording)      │
│  • Records a fragment via arecord | ffmpeg                       │
│  • Puts the finished file into work_queue                        │
└───────────────────────┬──────────────────────────────────────────┘
                        │ work_queue (Queue)
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Consumer thread (FileConsumer, daemon)                          │
│  ─────────────────────────────                                   │
│  • Takes a file from the queue                                   │
│  • Validates minimum size                                        │
│  • Moves it to the pending/ directory for upload                 │
└───────────────────────┬──────────────────────────────────────────┘
                        │ pending/*.mp3
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Upload thread (QueueProcessor, daemon)                          │
│  ─────────────────────────────                                   │
│  • Protected by a flock (single instance)                        │
│  • Checks internet connection (rclone about, cached)             │
│  • Uploads files to the cloud in parallel via rclone copy        │
│  • Each rclone call is bounded by --contimeout/--timeout         │
│  • Retries with configurable delays                              │
│  • Deletes files after successful upload                         │
└──────────────────────────────────────────────────────────────────┘
```

> **Key design decision:** the upload queue processor, the storage cleanup and the
> queue-size logging are all driven from the **top** of the producer loop, *before*
> the schedule check. The schedule restricts only the creation of new fragments.
> This is critical for a 24/7 device with an unstable link — a backlog built up
> during the day must be uploaded the moment the network recovers, even at 03:00.

### Data flow

```
output_dir/rec_20240101_120000.mp3  ──►  pending/rec_20240101_120000.mp3  ──►  Cloud
      (recording)                          (upload queue)                 (rclone copy)
```

---

## Requirements

### System utilities

| Utility | Package | Purpose |
|---|---|---|
| `arecord` | `alsa-utils` | Microphone audio capture |
| `ffmpeg` | `ffmpeg` | Encoding to opus/aac/mp3 |
| `rclone` | `rclone` | Cloud upload (not required when `cloud.service = "none"`) |
| `ping` | `iputils-ping` | Network speed estimation |

### Python libraries

| Library | Purpose |
|---|---|
| `pydantic` (v2) | Configuration validation |

The standard library (`os`, `sys`, `time`, `signal`, `threading`, `subprocess`, `logging`, `queue`, `concurrent.futures`, `pathlib`, `re`, `json`, `fcntl`, `shutil`, `datetime`, `typing`) is bundled with Python 3.8+.

> `fcntl` is Unix-only — the project targets Linux SBCs (Raspberry Pi). It will not run on Windows.

---

## Installation

### 1. Install system dependencies

```bash
sudo apt update
sudo apt install -y alsa-utils ffmpeg rclone python3-pydantic
```

### 2. Create a dedicated (non-root) user

For the principle of least privilege, the service runs as an unprivileged user
that belongs to the `audio` group (so it can access the microphone via ALSA):

```bash
sudo useradd -r -s /usr/sbin/nologin -G audio cloudrecorder
```

### 3. Configure the microphone

Verify that the system sees the microphone:

```bash
arecord -l
```

Find the card and device in the output, e.g. `hw:1,0`. Set this value in `config.json` → `audio.mic` (or use an ALSA mixer name).

### 4. Configure rclone (for cloud upload)

The rclone configuration must be created **for the `cloudrecorder` user**, because
the systemd service runs as that user and rclone looks up its config in
`~/.config/rclone/rclone.conf` (or `/home/cloudrecorder/.config/rclone/rclone.conf`):

```bash
sudo -u cloudrecorder rclone config
```

Create a remote whose name matches the `cloud.service` field in the config:

- for Yandex Disk — remote `yandexdisk`;
- for Google Drive — remote `googledrive`.

Verify it works as that user:

```bash
sudo -u cloudrecorder rclone about yandexdisk:
```

### 5. Deploy the files

```bash
sudo mkdir -p /opt/cloudrecorder
sudo cp cloudrecorder.py /opt/cloudrecorder/
sudo cp config.json    /opt/cloudrecorder/
sudo cp readme.md      /opt/cloudrecorder/
sudo chown -R cloudrecorder:cloudrecorder /opt/cloudrecorder
```

### 6. Install the systemd unit

```bash
sudo cp cloudrecorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cloudrecorder.service
```

---

## Configuration

All settings are configured via `config.json`. The configuration is validated by Pydantic models at startup — on error, the application prints a detailed message and exits with code 1.

### Full example with comments

```jsonc
{
  // Base paths
  "output_dir": "/opt/cloudrecorder",          // Working directory (recordings + pending/)
  "log_file":   "/opt/cloudrecorder/cloudrecorder.log",

  // Audio recording parameters
  "audio": {
    "split_time": 600,                          // Duration of one fragment, seconds
    "sample_rate": 48000,                       // Sample rate, Hz
    "sample_format": "S24_3LE",                 // ALSA sample format
    "mic": "default",                           // ALSA microphone device name
    "format": "mp3",                            // Target format: opus | aac | mp3
    "bitrate": 64,                              // Bitrate, kbps
    "file_prefix": "rec",                       // File name prefix
    "ffmpeg_timeout_grace_period": 20           // Extra seconds added to split_time for ffmpeg to finish
  },

  // Cloud upload
  "cloud": {
    "service": "yandex",                        // yandex | google | none
    "delete_after_upload": true,                // Delete local file after successful upload
    "retry_delay": 300,                         // Delay between attempts, normal network
    "max_retries": 15,                          // Max attempts, normal network
    "slow_network_retry_delay": 600,            // Delay between attempts, slow network
    "slow_network_max_retries": 5,              // Max attempts, slow network
    "network_speed_threshold": 100,             // Average ping threshold (ms): above = slow
    "max_parallel_uploads": 1,                  // Parallel upload threads on fast network
    "connectivity_timeout": 10,                 // rclone about timeout (seconds)
    "connectivity_check_interval": 180,         // Queue processor launch interval (seconds)
    "ping_address": "8.8.8.8"                   // Address for network speed estimation
  },

  // Storage limits
  "storage": {
    "max_mb": 25600                             // Size limit of the pending/ directory (MB)
  },

  // Google Drive settings (if service = "google")
  "google_drive": {
    "remote": "googledrive",
    "dir": "/Recordings"
  },

  // Yandex Disk settings (if service = "yandex")
  "yandex_disk": {
    "remote": "yandexdisk",
    "dir": "/Recordings"
  },

  // Recording schedule
  "schedule": {
    "enabled": true,                            // Enable time-window restriction
    "start_hour": 8,                            // Start hour (0–23)
    "end_hour": 18                              // End hour (0–23), must be > start_hour
  }
}
```

### Section reference

#### `audio`

| Field | Type | Default | Description |
|---|---|---|---|
| `split_time` | int (>0) | 600 | Duration of a single file, in seconds |
| `sample_rate` | int (>0) | 48000 | ALSA sample rate, Hz |
| `sample_format` | str | `S24_3LE` | ALSA sample format (S16_LE, S24_3LE, S32_LE, …) |
| `mic` | str | `default` | ALSA device name (`default`, `hw:1,0`, `plughw:1,0`, a name from `.asoundrc`) |
| `format` | `opus`\|`aac`\|`mp3` | `mp3` | Target encoding format |
| `bitrate` | int (>0) | 64 | Bitrate, kbps |
| `file_prefix` | str | `rec` | File name prefix (`<prefix>_YYYYMMDD_HHMMSS.<ext>`). Only `[A-Za-z0-9_-]` allowed |
| `ffmpeg_timeout_grace_period` | int (≥5) | 20 | Extra seconds added to `split_time` for encoding to finish |

#### `cloud`

| Field | Type | Default | Description |
|---|---|---|---|
| `service` | `yandex`\|`google`\|`none` | `yandex` | Cloud provider. `none` — local storage only |
| `delete_after_upload` | bool | `true` | Delete the local file after a successful upload |
| `retry_delay` | int (≥0) | 300 | Delay between attempts (seconds), normal network |
| `max_retries` | int (≥0) | 15 | Max attempts, normal network |
| `slow_network_retry_delay` | int (≥0) | 600 | Delay between attempts (seconds), slow network |
| `slow_network_max_retries` | int (≥0) | 5 | Max attempts, slow network |
| `network_speed_threshold` | int (>0) | 100 | Average ping threshold (ms); above → network is considered slow |
| `max_parallel_uploads` | int (>0) | 1 | Number of parallel upload threads on a **fast** network. Forced to 1 on `slow`/`unknown` |
| `connectivity_timeout` | int (>0) | 10 | Cloud availability check timeout via `rclone about` (seconds) |
| `connectivity_check_interval` | int (≥0) | 180 | Queue processor launch interval (seconds) |
| `ping_address` | str | `8.8.8.8` | IP/host for network speed estimation |

> **rclone I/O timeouts are not in the config** — they are hardcoded as
> `--contimeout=15s --timeout=60s --low-level-retries=5` (see `RCLONE_IO_FLAGS` in
> `cloudrecorder.py`) plus `--retries=1` on `rclone copy`. This guarantees that a
> single upload attempt cannot hang forever on an unstable link. Tune the source
> constant if your network requires different values.

#### `storage`

| Field | Type | Default | Description |
|---|---|---|---|
| `max_mb` | int (>0) | 25600 | Size limit of the `pending/` directory in MB. When exceeded, the oldest files are deleted |

#### `google_drive` / `yandex_disk`

| Field | Type | Default | Description |
|---|---|---|---|
| `remote` | str | `googledrive` / `yandexdisk` | rclone remote name (from `rclone config`) |
| `dir` | str | `/Recordings` | Path to the folder in the cloud |

#### `schedule`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Enable recording time-window restriction |
| `start_hour` | int (0–23) | 8 | Start hour of the recording window |
| `end_hour` | int (0–23) | 20 | End hour of the recording window (must be greater than `start_hour`) |

> **Limitation:** overnight windows (`start_hour > end_hour`, e.g. 22→06) are **not**
> supported — the validator requires `end_hour > start_hour`. To record around the
> clock, set `enabled: false`. The upload queue and storage cleanup run **regardless**
> of the schedule, so this restriction affects only when new fragments are created.
> The schedule uses the system local timezone — make sure the Pi's clock and timezone
> are correct (`sudo raspi-config` → Localisation Options).

---

## Running

### Manual run

```bash
cd /opt/cloudrecorder
sudo -u cloudrecorder python3 cloudrecorder.py config.json
```

If the config path is not provided, `config.json` in the current directory is used.

### Run as a systemd service

The unit file is installed in step 6 of the Installation section. After changes:

```bash
sudo systemctl daemon-reload
sudo systemctl restart cloudrecorder.service
```

Check the status:

```bash
sudo systemctl status cloudrecorder.service
```

View the service logs:

```bash
sudo journalctl -u cloudrecorder.service -f
```

Contents of `cloudrecorder.service`:

```ini
[Unit]
Description=CloudRecorder - Audio Recorder with Cloud Upload
Documentation=file:///opt/cloudrecorder/readme.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# Non-root, least privilege. User must be in the audio group for ALSA access.
User=cloudrecorder
Group=cloudrecorder
SupplementaryGroups=audio
WorkingDirectory=/opt/cloudrecorder
# Default: Python interpreter. Swap to cloudrecorder.bin if you built the Nuitka binary.
ExecStart=/usr/bin/python3 /opt/cloudrecorder/cloudrecorder.py /opt/cloudrecorder/config.json
Environment=PYTHONUNBUFFERED=1
Restart=always
RestartSec=10
TimeoutStopSec=30
KillMode=control-group
KillSignal=SIGTERM
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Key choices:

- **`User=cloudrecorder`** — runs unprivileged; the user is created with `-G audio` so it can open the microphone.
- **`Restart=always`** — the service is restarted on *any* exit (not just failure), which is what you want for a 24/7 unattended device.
- **`TimeoutStopSec=30`** + **`KillMode=control-group`** — give graceful shutdown up to 30 s, then SIGKILL the whole cgroup (no orphan `arecord`/`ffmpeg`/`rclone`).
- **`After=network-online.target` + `Wants=network-online.target`** — wait for the network stack to be ready before starting.
- **`Environment=PYTHONUNBUFFERED=1`** — logs reach journald without buffering delay.

---

## Directory structure

After the first launch, the following structure is created in `output_dir`:

```
/opt/cloudrecorder/
├── cloudrecorder.py            # Main script (or cloudrecorder.bin if built with Nuitka)
├── config.json                 # Configuration
├── readme.md                   # This document
├── cloudrecorder.log           # Current log
├── cloudrecorder.log.1         # Yesterday's log (rotated)
├── rec_20240101_120000.mp3     # Active/fresh recording (moved to pending after processing)
├── upload.lock                 # Queue processor lock file (held open via flock)
└── pending/                    # Queue of files pending upload
    ├── rec_20240101_110000.mp3
    ├── rec_20240101_113000.mp3
    └── ...
```

---

## Logging

- Logs are written simultaneously to a file (`log_file`) and to `stdout`.
- Format: `YYYY-MM-DD HH:MM:SS,mmm - LEVEL - ThreadName - message`.
- Rotation: daily at midnight, retaining logs for the last **14 days** (`LOG_RETENTION_DAYS`).
- Log level: `INFO`.
- **Asynchronous**: all log records go through a `QueueHandler` into an in-memory queue, and a dedicated `QueueListener` thread writes them to the file. This keeps the number of `write()`/`fsync()` calls on the micro-SD low during 24/7 operation. On shutdown, `QueueListener.stop()` flushes the remaining buffer before the process exits.

Example log entries:

```
2024-01-01 12:00:00,123 - INFO - MainThread      - ▶ Запуск записи в формате mp3 с выгрузкой на Яндекс.Диск
2024-01-01 12:00:00,456 - INFO - MainThread      - Начало записи: rec_20240101_120000.mp3
2024-01-01 12:10:05,789 - INFO - MainThread      - Запись завершена: /opt/cloudrecorder/rec_20240101_120000.mp3
2024-01-01 12:10:06,012 - INFO - FileConsumer    - Потребитель получил файл: rec_20240101_120000.mp3
2024-01-01 12:10:06,234 - INFO - FileConsumer    - Файл добавлен в очередь: /opt/cloudrecorder/pending/rec_20240101_120000.mp3
2024-01-01 12:13:00,567 - INFO - QueueProcessor  - Обработка очереди (1 файлов, сеть: fast, потоков: 1)
2024-01-01 12:13:45,890 - INFO - Uploader_0      - Успешно загружено: /opt/cloudrecorder/pending/rec_20240101_120000.mp3
2024-01-01 12:13:45,901 - INFO - Uploader_0      - Файл удалён: /opt/cloudrecorder/pending/rec_20240101_120000.mp3
```

---

## Failure behavior

### Crash during recording

- Orphaned recording files may be left in `output_dir`.
- On the next launch, `recover_interrupted_files()`:
  - files larger than `MIN_FILE_SIZE_BYTES` (1024 bytes) are queued for processing;
  - smaller files are deleted as corrupted.
- Active recording subprocesses are tracked in memory; on graceful shutdown they receive `SIGTERM` and are reaped in a `finally` block. On a hard crash / SIGKILL, `arecord` self-terminates after its `-d` duration and `ffmpeg` finishes on EOF.

### Internet connection loss

- When the cloud is unreachable (`rclone about` does not respond), the queue processor skips the cycle. The negative result is cached for only **15 s** (vs. 60 s for a positive result), so recovery is detected quickly on a flaky link.
- Files remain in `pending/` until the connection is restored.
- The upload queue processor keeps running on its interval **even outside the recording schedule**, so a backlog is drained the moment the network is back.
- If `pending/` exceeds the `storage.max_mb` limit, the oldest files are automatically deleted.

### Stuck / slow rclone process

- Every `rclone copy` and `rclone about` call is launched with `--contimeout` and `--timeout`, so a single attempt can never block forever.
- `rclone copy` is launched with `--retries=1` (rclone does not retry internally); our own retry loop in `upload_to_cloud()` controls delays and attempt counts.
- `upload_to_cloud()` checks `shutdown_event` between attempts, so a shutdown is not delayed by retry sleeps.
- On service stop, systemd's `KillMode=control-group` sends SIGTERM/SIGKILL to the whole cgroup, cleaning up any in-flight rclone.

### Critically low disk space

- Free space is checked before each new recording.
- Threshold: `min(10% of total, 1 GB)` of free space (`DISK_FREE_PERCENTAGE`, `DISK_FREE_MIN_BYTES`).
- When insufficient, recording is paused until space is freed. The upload queue continues to run and delete uploaded files, which itself frees space.

### Graceful shutdown (SIGINT / SIGTERM)

- The signal handler is intentionally **minimal**: it sets `shutdown_event` and terminates the active `arecord`/`ffmpeg` subprocesses. It does **not** call `join()` — calling blocking operations from a signal handler (which runs in the main thread) would delay the very thread it is trying to unblock.
- `shutdown_event` propagates to all loops: the producer exits after the current recording, the consumer drains its current item, the upload thread finishes its current cycle.
- All `time.sleep` waits are replaced with interruptible `shutdown_event.wait()`.
- `run()` joins the consumer and upload threads with a 10 s timeout each; if they do not finish, a warning is logged (the process exits anyway, since all worker threads are daemons). systemd's `TimeoutStopSec=30` gives the whole shutdown up to 30 s before SIGKILL.

### Local-only mode (`cloud.service = "none"`)

When `cloud.service = "none"`, files are **not** uploaded anywhere and stay in `output_dir`. To distinguish already-processed files from unfinished recordings on restart, processed files get a `.done` suffix appended to their extension (e.g. `rec_20240101_120000.mp3.done`). `recover_interrupted_files()` globs `rec_*.mp3`, which never matches `.done` files, so they are skipped — no reprocessing loop.

---

## Reliability notes for Raspberry Pi Zero 2 W

The Pi Zero 2 W has a 1 GHz quad-core ARM Cortex-A53 CPU and **512 MB RAM**, boots from a micro-SD card, and is typically powered from a phone charger. Recommendations:

- **RAM**: keep `cloud.max_parallel_uploads = 1` (the default). Each `rclone` process uses 50–100 MB; on 512 MB several parallel uploads plus `ffmpeg` encoding can trigger the OOM killer. The code already forces 1 thread on `slow`/`unknown` networks.
- **CPU**: `arecord -f S24_3LE -r 48000` + `ffmpeg` libmp3lame encoding in real time is feasible on the Zero 2 W, but for pure speech recording you can drop to `sample_rate: 16000` / `sample_format: S16_LE` to leave more headroom.
- **micro-SD wear**: logging is asynchronous (batched via `QueueListener`); the `.recording` marker files were removed; `shutil.move` reduces to an atomic `rename` on the same filesystem; `_cleanup_storage` is throttled to one scan per 60 s. For maximum endurance, consider an industrial-grade micro-SD or booting from USB/SSD.
- **Power loss**: ext4 is journaled, but a sudden power cut can still corrupt an open file. The design mitigates this — recordings are streamed through a pipe and only moved/renamed once complete, `rclone copy` is idempotent, and `delete_after_upload` only fires on `returncode == 0`. Consider enabling the Pi's read-only-overlay mode if writes are infrequent, or adding a small UPS / supercapacitor hat.
- **Clock**: install `chrony` or `systemd-timesyncd` so the Pi's clock is correct after a power loss (the Pi has no RTC by default). The recording schedule relies on local time.
- **Watchdog** (optional, not bundled): enable the hardware watchdog (`sudo raspi-config` → Interface Options → Watchdog) and add `WatchdogSec=300` + `sd_notify` support if you need automatic reboot on a full software freeze.

---

## Building a binary (optional)

For deployment on the target device, the script can be compiled into a single binary using **Nuitka**:

```bash
# Install Nuitka
pipx install nuitka

# Compile
nuitka --lto=yes cloudrecorder.py
```

The result is a single executable file `cloudrecorder.bin` that is deployed alongside `config.json`. If you use the binary, update `ExecStart=` in `cloudrecorder.service` to point at `/opt/cloudrecorder/cloudrecorder.bin`.

---

## Project file structure

```
cloudrecorder/
├── cloudrecorder.py        # Main script (single file)
├── config.json             # Default configuration
├── cloudrecorder.service   # systemd unit file
├── readme.md               # This document
└── LICENSE                 # GNU GPL v3
```

---

## License

GNU GPL v3
