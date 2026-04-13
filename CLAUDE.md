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

- `hpc_web/app.py` — Flask + Flask-SocketIO app on the HPC side; launches receiver subprocess, tracks progress, displays received images
- `rdb_web/app_rdb.py` — Flask app on the RDB side; scans for files, launches sender
- `app_temp.py` / `app_temp_delay_results.py` — alternate HPC interface variants

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
