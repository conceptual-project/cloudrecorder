# CloudRecorder

**Automatic microphone audio recording with cloud upload of recorded files.**

CloudRecorder is a standalone Python application designed for long-term autonomous audio recording on Raspberry Pi or any other SBC (Single-Board Computer). The program records audio in fragments of a specified duration, queues the finished files, and asynchronously uploads them to the cloud (Yandex Disk or Google Drive) via `rclone`.

---

## Features

- **Audio recording** from a microphone via `arecord` (ALSA) with on-the-fly `ffmpeg` transcoding.
- **Encoding formats** to choose from: `opus`, `aac`, `mp3` (mono, configurable bitrate).
- **Fragmentation** — recording is split into files of fixed duration (10 minutes by default).
- **Cloud upload** via `rclone` (Yandex Disk, Google Drive, or disabled — local storage only).
- **Network speed adaptation**: measures average ping, with separate retry limits and delays for slow connections.
- **Parallel upload** of multiple files at once (`ThreadPoolExecutor`) on fast networks.
- **Schedule-based operation** — record only within a specified time window (e.g., 08:00–20:00).
- **Free space monitoring**: when the storage limit is exceeded, the oldest files are automatically deleted from the queue. The default parameters assume the system has 32 GB of free space.
- **Crash recovery** — on restart, unfinished recordings and files left in the upload queue are handled correctly.
- **Log rotation** — daily, retaining logs for the last 14 days.
- **Graceful shutdown** on `SIGINT` / `SIGTERM` signals, waiting for in-flight tasks to complete.
- **Runs as a systemd service**.

---

## Architecture

The application follows a **producer–consumer** pattern with three threads:

```
┌──────────────────────────────────────────────────────────────────┐
│  Main thread (Producer)                                          │
│  ─────────────────────────────                                   │
│  • Checks schedule and free disk space                           │
│  • Launches the upload queue processor (on interval)             │
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
│  • Protected by a lock file (single instance)                    │
│  • Checks internet connection and network speed                  │
│  • Uploads files to the cloud in parallel via rclone copy        │
│  • Retries with configurable delays                              │
│  • Deletes files after successful upload                         │
└──────────────────────────────────────────────────────────────────┘
```

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

The standard library (`os`, `sys`, `threading`, `subprocess`, `logging`, `queue`, `concurrent.futures`, `pathlib`, `re`, `json`) is bundled with Python 3.8+.

---

## Installation

### 1. Install system dependencies

```bash
sudo apt update
sudo apt install -y alsa-utils ffmpeg rclone python3-pydantic
```

### 2. Configure the microphone

Verify that the system sees the microphone:

```bash
arecord -l
```

Find the card and device in the output, e.g. `hw:1,0`. Set this value in `config.json` → `audio.mic` (or use an ALSA mixer name).

### 3. Configure rclone (for cloud upload)

```bash
rclone config
```

Create a remote whose name matches the `cloud.service` field in the config:

- for Yandex Disk — remote `yandexdisk`;
- for Google Drive — remote `googledrive`.

### 4. Deploy the files

```bash
sudo mkdir -p /opt/cloudrecorder
sudo cp cloudrecorder.py /opt/cloudrecorder/
sudo cp config.json    /opt/cloudrecorder/
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
| `file_prefix` | str | `rec` | File name prefix (`<prefix>_YYYYMMDD_HHMMSS.<ext>`) |
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
| `max_parallel_uploads` | int (>0) | 1 | Number of parallel upload threads (forced to 1 on slow networks) |
| `connectivity_timeout` | int (>0) | 10 | Cloud availability check timeout via `rclone about` (seconds) |
| `connectivity_check_interval` | int (≥0) | 180 | Queue processor launch interval (seconds) |
| `ping_address` | str | `8.8.8.8` | IP/host for network speed estimation |

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

---

## Running

### Manual run

```bash
cd /opt/cloudrecorder
python3 cloudrecorder.py config.json
```

If the config path is not provided, `config.json` in the current directory is used.

### Run as a systemd service

1. Copy the unit file:

   ```bash
   sudo cp cloudrecorder.service /etc/systemd/system/
   ```

2. Reload the systemd configuration and enable autostart:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now cloudrecorder.service
   ```

3. Check the status:

   ```bash
   sudo systemctl status cloudrecorder.service
   ```

4. View the service logs:

   ```bash
   sudo journalctl -u cloudrecorder.service -f
   ```

Contents of `cloudrecorder.service`:

```ini
[Unit]
Description=CloudRecorder - Audio Recorder with Cloud Upload
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/cloudrecorder
ExecStart=/opt/cloudrecorder/cloudrecorder.bin /opt/cloudrecorder/config.json
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## Directory structure

After the first launch, the following structure is created in `output_dir`:

```
/opt/cloudrecorder/
├── cloudrecorder.py            # Executable script (or cloudrecorder.bin)
├── config.json                 # Configuration
├── cloudrecorder.log           # Current log
├── cloudrecorder.log.1         # Yesterday's log (rotated)
├── rec_20240101_120000.mp3     # Active/fresh recording (moved to pending after processing)
├── upload.lock                 # Queue processor lock file (temporary)
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

Example log entries:

```
2024-01-01 12:00:00,123 - INFO - MainThread      - ▶ Starting recording in mp3 format with upload to Yandex Disk
2024-01-01 12:00:00,456 - INFO - MainThread      - Recording started: rec_20240101_120000.mp3
2024-01-01 12:10:05,789 - INFO - MainThread      - Recording finished: /opt/cloudrecorder/rec_20240101_120000.mp3
2024-01-01 12:10:06,012 - INFO - FileConsumer    - Consumer received file: rec_20240101_120000.mp3
2024-01-01 12:10:06,234 - INFO - FileConsumer    - File added to queue: /opt/cloudrecorder/pending/rec_20240101_120000.mp3
2024-01-01 12:13:00,567 - INFO - QueueProcessor  - Processing queue (1 files, network: fast, threads: 1)
2024-01-01 12:13:45,890 - INFO - Uploader_0      - Successfully uploaded: /opt/cloudrecorder/pending/rec_20240101_120000.mp3
2024-01-01 12:13:45,901 - INFO - Uploader_0      - File deleted: /opt/cloudrecorder/pending/rec_20240101_120000.mp3
```

---

## Failure behavior

### Crash during recording

- Orphaned recording files may be left in `output_dir`.
- On the next launch, `recover_interrupted_files()`:
  - files larger than `MIN_FILE_SIZE_BYTES` (1024 bytes) are queued for processing;
  - smaller files are deleted as corrupted.
- Active recording subprocesses are tracked in memory; on graceful shutdown they receive `SIGTERM` and are reaped.

### Internet connection loss

- When the cloud is unreachable (`rclone about` does not respond), the queue processor skips the cycle.
- Files remain in `pending/` until the connection is restored.
- If `pending/` exceeds the `storage.max_mb` limit, the oldest files are automatically deleted.

### Critically low disk space

- Free space is checked before each new recording.
- Threshold: `min(10% of total, 1 GB)` of free space (`DISK_FREE_PERCENTAGE`, `DISK_FREE_MIN_BYTES`).
- When insufficient, recording is paused until space is freed.

### Graceful shutdown (SIGINT / SIGTERM)

- `shutdown_event` is set.
- Active `arecord`/`ffmpeg` subprocesses receive `SIGTERM`.
- The current upload cycle is awaited with a 10-second timeout.
- All waits (`time.sleep`) are replaced with interruptible `shutdown_event.wait()`, ensuring a fast and clean exit.

---

## Building a binary (optional)

For deployment on the target device, the script can be compiled into a single binary using **Nuitka**:

```bash
# Install Nuitka
pipx install nuitka

# Compile
nuitka --lto=yes cloudrecorder.py
```

The result is a single executable file `cloudrecorder.bin` that is deployed alongside `config.json`. See `readme.html` for details.

---

## Project file structure

```
cloudrecorder/
├── cloudrecorder.py        # Main script (~890 lines, single file)
├── config.json             # Default configuration
├── cloudrecorder.service   # systemd unit file
└── readme.html             # Binary build and deployment guide
```

---

## License

GNU GPL v3
