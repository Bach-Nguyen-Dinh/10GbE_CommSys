# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance file transfer system for 10 Gigabit Ethernet networks. Implements a sender-receiver model in C with parallel TCP streams, zero-copy optimizations, and adaptive chunk sizing. Includes Flask-based web interfaces for monitoring and control.

## Architecture

### Sender-Receiver Model

Two machines communicate over a 10GbE link:
- **RDB (sender)**: `169.254.207.123` — transmits files using parallel TCP streams
- **HPC (receiver)**: `169.254.207.40` — receives files, reports progress via UDP

### Implementation Tiers

**Simple (baseline):**
- `simple_sender_rdb.c` / `simple_receiver_hpc.c`
- Fixed chunk size (32MB), lock-free work queue, configurable zero-copy modes
- Supports repeated transfers with per-repeat metrics

**Adaptive (WIP):**
- `file_sender_rdb_controlled_adaptive_v3_WIP.c` — adaptive sender with dynamic chunk sizing
- `file_receiver_hpc_controlled_adaptive_v3.c` / `*_WIP.c` / `*_WIP_SocketForSynchronization.c` — receiver variants
- Adds: persistent connections, adaptive chunk sizing based on throughput monitoring, command port (5400) for synchronization, multi-file orchestration with READY/COMPLETE signals

### Network Protocol

- **Base port** (default 5303): file header exchange and control signaling
- **Data ports** (base+1 through base+N): one per parallel stream/thread
- **UDP progress port** (5666): receiver sends progress updates to web UI
- File header magic: `0xdeadbeefcafebabe`
- Chunk headers carry `(chunk_id, offset, size)` for reassembly
- Ready (`0xAA`) / Ack (`0xAB`) / Heartbeat (`0xBB`) signal protocol

### Web Interfaces (`demo_interface/`)

Two Flask apps drive an end-to-end demo. The HPC app is the user-facing UI; it
orchestrates the RDB app over HTTP:

- `hpc_web/app.py` — Flask + Flask-SocketIO UI, serves on **port 5110**. Launches
  the receiver subprocess, parses its stdout for READY/COMPLETE/TRANSFER signals,
  and pushes live thumbnail/progress updates to the browser over SocketIO.
  Watches the received-files directory with `watchdog`.
- `rdb_web/app_rdb.py` — headless Flask REST service on the RDB side, serves on
  **port 5111**. Scans `demo_resources/` for files and runs `simple_sender_rdb`,
  parsing the sender's `=== FINAL AVERAGE RESULTS ===` block into JSON metrics.
- `hpc_web/app_temp.py` / `app_temp_delay_results.py` — alternate HPC UI variants
  (`app_temp_delay_results.py` is the one with TIF-file support).

**Demo flow:** browser → HPC `app.py` starts receiver → HPC POSTs `/api/send-file`
to RDB `app_rdb.py` → RDB runs the sender and returns averaged metrics → HPC relays
metrics + latest thumbnail to the browser via SocketIO.

**Key REST endpoints (RDB `app_rdb.py`):** `GET /api/scan-files` (PNG),
`GET /api/scan-tif-files` (recursive `.tif`/`.tiff` scan of `demo_resources/`),
`POST /api/send-file`, `POST /api/stop-transfer`, `GET /api/status`, `GET /health`.

**Note on ports:** the web demo hard-codes the receiver port as **5202** and the
receiver binary as `file_receiver_hpc_controlled` (not the `5303` / `simple_*`
defaults used for manual CLI runs). Update the constants at the top of each
`app*.py` (`RECEIVER_PORT`, `RECEIVER_BINARY`, `RDB_FLASK_URL`, `HPC_IP`) to match
your setup.

- `demo_resources/` (repo root, not tracked) — source PNG/TIF files the RDB app
  scans and sends; resolved two levels up from `rdb_web/`.
- `hpc_web/templates/` — `index.html` (used by `app.py`) and `index_temp.html`
  (used by the temp variants); thumbnails served from `hpc_web/static/thumbnails/`.

## Build Commands

All C programs compile with the same flags:

```bash
gcc -O3 -Wall -pthread -std=gnu11 -o <binary> <source>.c -lm
```

### Specific builds

```bash
# Simple sender
gcc -O3 -Wall -pthread -std=gnu11 -o simple_sender_rdb simple_sender_rdb.c -lm

# Simple receiver
gcc -O3 -Wall -pthread -std=gnu11 -o simple_receiver_hpc simple_receiver_hpc.c -lm

# Adaptive sender
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_sender file_sender_rdb_controlled_adaptive_v3_WIP.c -lm

# Adaptive receiver (pick the variant you need)
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_receiver file_receiver_hpc_controlled_adaptive_v3.c -lm
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_receiver file_receiver_hpc_controlled_adaptive_v3_WIP.c -lm
```

## Running

### Simple transfer

```bash
# On HPC (receiver) — start first
./simple_receiver_hpc 5303 4 1 ./
#                     port streams save_to_disk output_dir

# On RDB (sender)
./simple_sender_rdb 169.254.207.40 my.bin -s 4 -z 1 -p 5303 -r 5
#                   receiver_ip    file   streams zerocopy port repeats
# zerocopy modes: 0=writev, 1=sendfile, 2=MSG_ZEROCOPY
```

### Web interfaces

```bash
# HPC side
cd demo_interface/hpc_web && python3 app.py

# RDB side
cd demo_interface/rdb_web && python3 app_rdb.py
```

## Key Implementation Details

- **Lock-free work queue**: uses `_Atomic int` for chunk distribution across threads; no mutexes on the data path
- **Zero-copy modes**: writev (fallback), sendfile (default), MSG_ZEROCOPY — selected via `-z` flag on sender
- **Barrier synchronization**: `pthread_barrier_t` coordinates threads between repeats and between files
- **CPU affinity**: threads pinned to specific cores via `sched_setaffinity` for cache locality
- **Socket tuning**: TCP_NODELAY enabled; send buffers 4MB, receive buffers up to 128MB
- **Adaptive chunk sizing** (WIP variants): monitors per-thread throughput with rolling averages and adjusts chunk sizes dynamically

## Python Dependencies (Web Interfaces)

flask, flask_socketio, requests, watchdog
