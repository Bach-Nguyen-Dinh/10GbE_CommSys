# 10GbE_CommSys

High-performance file transfer system for 10 Gigabit Ethernet networks. Implements a sender-receiver model in C with parallel TCP streams, zero-copy optimizations, and adaptive chunk sizing. Includes Flask-based web interfaces for monitoring and control.

## Architecture

Two machines communicate over a direct 10GbE link:

| Role | Host | IP |
|------|------|----|
| Sender (RDB) | transmits files via parallel TCP streams | `169.254.207.123` |
| Receiver (HPC) | receives files, reports progress via UDP | `169.254.207.40` |

### Network Protocol

- **Base port** (default `5303`): file header exchange and control signaling
- **Data ports** (`base+1` through `base+N`): one per parallel stream/thread
- **UDP progress port** (`5666`): receiver streams progress updates to the web UI
- File header magic: `0xdeadbeefcafebabe`
- Chunk headers carry `(chunk_id, offset, size)` for out-of-order reassembly
- Ready (`0xAA`) / Ack (`0xAB`) / Heartbeat (`0xBB`) signal protocol

## Implementation Tiers

### Simple (baseline)

`simple_sender_rdb.c` / `simple_receiver_hpc.c`

- Fixed 32 MB chunk size
- Lock-free work queue using `_Atomic int` — no mutexes on the data path
- Configurable zero-copy modes (`writev`, `sendfile`, `MSG_ZEROCOPY`)
- Per-repeat throughput metrics across configurable repeat counts

### Adaptive (WIP)

`file_sender_rdb_controlled_adaptive_v3_WIP.c` + receiver variants

- Persistent connections across multiple file transfers
- Dynamic chunk sizing driven by per-thread rolling throughput averages
- Command port `5400` for sender/receiver synchronization
- Multi-file orchestration with READY/COMPLETE signaling

## Build

All C programs use the same flags:

```bash
gcc -O3 -Wall -pthread -std=gnu11 -o <binary> <source>.c -lm
```

Specific targets:

```bash
# Simple sender / receiver
gcc -O3 -Wall -pthread -std=gnu11 -o simple_sender_rdb simple_sender_rdb.c -lm
gcc -O3 -Wall -pthread -std=gnu11 -o simple_receiver_hpc simple_receiver_hpc.c -lm

# Adaptive sender
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_sender file_sender_rdb_controlled_adaptive_v3_WIP.c -lm

# Adaptive receiver (pick the variant you need)
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_receiver file_receiver_hpc_controlled_adaptive_v3.c -lm
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_receiver file_receiver_hpc_controlled_adaptive_v3_WIP.c -lm
```

## Usage

### Simple transfer

```bash
# On HPC (receiver) — start first
./simple_receiver_hpc <port> <streams> <save_to_disk> <output_dir>
./simple_receiver_hpc 5303 4 1 ./

# On RDB (sender)
./simple_sender_rdb <receiver_ip> <file> -s <streams> -z <zerocopy> -p <port> -r <repeats>
./simple_sender_rdb 169.254.207.40 my.bin -s 4 -z 1 -p 5303 -r 5
```

Zero-copy modes (`-z`):

| Value | Mode |
|-------|------|
| `0` | `writev` (fallback) |
| `1` | `sendfile` (default) |
| `2` | `MSG_ZEROCOPY` |

### Web interfaces

```bash
# HPC side — launches receiver subprocess, tracks progress, displays received images
cd demo_interface/hpc_web && python3 app.py

# RDB side — scans for files, launches sender
cd demo_interface/rdb_web && python3 app_rdb.py
```

Python dependencies: `flask`, `flask_socketio`, `requests`, `watchdog`

## Key Implementation Details

- **Lock-free work queue**: chunk distribution across threads via `_Atomic int`; no mutex contention on the hot path
- **Zero-copy I/O**: three selectable modes — `writev`, `sendfile`, `MSG_ZEROCOPY`
- **Barrier synchronization**: `pthread_barrier_t` coordinates threads between repeats and file transfers
- **CPU affinity**: threads pinned to specific cores via `sched_setaffinity` for cache locality
- **Socket tuning**: `TCP_NODELAY` enabled; send buffers 4 MB, receive buffers up to 128 MB
- **Adaptive chunk sizing** (WIP): per-thread throughput monitored with rolling averages; chunk sizes adjust dynamically
