# Adaptive v3 (WIP) Sender/Receiver — Problems and How They Were Solved

This document explains how `file_sender_rdb_controlled_adaptive_v3_WIP.c` (runs on
**RDB**, `169.254.207.123`) and `file_receiver_hpc_controlled_adaptive_v3_WIP.c`
(runs on **HPC**, `169.254.207.40`) cooperate to move the same file repeatedly over
a 10 GbE link. It is structured like a STAR interview answer: each section states a
**Problem** (the situation and what had to be achieved), the **Action** taken in
the code, and the **Result** — what that bought, with the theory behind it. It
assumes you can read C but not that you know POSIX networking or threading
internals.

> **Important framing:** in this WIP variant the receiver **does not write anything
> to disk**. It reads bytes into a scratch buffer and throws them away. The pair is
> effectively a *throughput benchmark with file-transfer semantics* (real headers,
> real synchronization, real per-file accounting) — the "save the file" step from
> other receiver variants has been removed. See [Open problems](#e-open-problems--not-yet-solved).

---

## The situation: what the system has to do

Move one file from RDB to HPC over a back-to-back 10 GbE link, repeatedly
(`max_files` times), measuring throughput per file — as fast as the link allows,
without ever corrupting framing or deadlocking between files.

```
        RDB (sender)                                HPC (receiver)
  ┌──────────────────────┐                    ┌──────────────────────┐
  │ main()               │                    │ main()               │
  │  mmap() the file     │                    │  create N listeners  │
  │  spawn N send threads│   DATA (TCP)       │  spawn N recv threads│
  │                      │ ================>  │                      │
  │  thread 0 ── port 5303 ─────────────────► thread 0 (also reads   │
  │  thread 1 ── port 5304 ─────────────────► thread 1   the header) │
  │  thread 2 ── port 5305 ─────────────────► thread 2              │
  │  thread 3 ── port 5306 ─────────────────► thread 3              │
  │                      │                    │                      │
  │ ready_signal_listener│   CONTROL (TCP)    │ ready_signaling_thread
  │  listens on 5403 ◄══════════════════════ │  connects to sender  │
  │  READY(0xAA) ◄─────── │                   │  sends READY per file│
  │  ACK(0xAB)   ───────► │                   │  waits for ACK       │
  └──────────────────────┘                    └──────────────────────┘
```

| Channel | Who listens | Who connects | Ports | Purpose |
|---|---|---|---|---|
| **Data** (N connections) | Receiver (HPC) | Sender (RDB) | `base_port + thread_id` (default 5303..5306) | Bulk file bytes, plus the per-file header on thread 0's connection |
| **Control** (1 connection) | Sender (RDB) | Receiver (HPC) | `base_port + 100` (5403 by default) | READY / ACK / HEARTBEAT signaling between files |

Every design decision below exists because the naive version of this task fails in
a specific way. The problems fall into five groups, plus one worked trace of the
literal bytes on the wire:

- **[A. Throughput](#a-throughput-problems)** — why a simple `read()`/`send()` loop can't fill 10 Gb/s.
- **[B. Correctness over a byte stream](#b-correctness-problems)** — why TCP alone doesn't keep two programs in agreement.
- **[C. Coordination between files](#c-coordination-problems)** — when may the sender start the next file?
- **[D. Bugs found and fixed](#d-bugs-found-in-review--and-their-fixes)** — defects in the original snapshot and their remedies.
- **[E. Open problems](#e-open-problems--not-yet-solved)** — what genuinely remains unfinished.
- **[Wire sequence, worked example](#wire-sequence--worked-example-500-mb--3-repeats-4-threads-16-mb-grain)** — what is sent first, its exact content, and every send after that, for a concrete 500 MB × 3 run.

---

## Wire sequence — worked example: 500 MB × 3 repeats, 4 threads, 16 MB grain

Sections A–E explain *why* each mechanism exists. This section answers a
narrower question: for one concrete run, **what actually crosses the wire, in
what order, with what bytes** — first send, its content, and every send after
that.

```bash
./adaptive_receiver 5303 4 3 169.254.207.123
./adaptive_sender   169.254.207.40 demo.png 4 1 5303 3
```

500 MB file (524,288,000 bytes, filename `demo.png`, 8 characters), 4 streams
(data ports 5303–5306, control port 5403), 16 MiB grain → the file splits into
**32 work items**: 31 full 16 MiB items + one 4 MiB remainder
(524,288,000 = 31 × 16,777,216 + 4,194,304). `max_files = 3`, so the same file
goes over the same connections three times.

### Phase 0 — connection setup (no file bytes yet)

| # | Who | Action | Wire content |
|---|---|---|---|
| 1 | Receiver | binds+listens on 5303, 5304, 5305, 5306; 4 threads block in `accept()` | — |
| 2 | Sender | `mmap`s `demo.png`; spawns `ready_signal_listener`, which binds+listens on 5403; `sleep(1)` | — |
| 3 | Sender | connects stream 0→5303, 1→5304, 2→5305, 3→5306, in that order | TCP handshake only |
| 4 | Receiver | all 4 `accept()`s return, `client_fd[0..3]` set (main busy-polls until so) | — |
| 5 | Receiver | connects out to the sender's control port 5403 | TCP handshake only |
| 6 | Sender | `ready_signal_listener` accepts that connection | — |
| 7 | Receiver | `main` sets `file_processing_done = 1` immediately, before file 1 even starts; `ready_signaling_thread` wakes | — |

### Phase 1 — the first payload bytes of the run

```
[control]  Receiver → Sender   1 B    0xAA  READY          (fired for "file 1" — see note)
[control]  Sender   → Receiver 1 B    0xAB  ACK
[data 0]   Sender   → Receiver 36 B   HEADER for file 1:
             8 B  sync marker   0xDEADBEEFCAFEBABE   (htobe64)
             4 B  file_num      1                    (htonl)
             8 B  file_size     524288000            (htobe64)
             4 B  num_streams   4                     (htonl)
             4 B  filename_len  8                     (htonl)
             8 B  filename      "demo.png"            (raw bytes, no NUL)
```

**Note on the file-1 READY:** the sender's `file_num == 1` path deliberately
never waits for it (B4/C1 skip the wait for the first file) — thread 0 sends
the header as soon as its 4 data sockets are connected. The byte still arrives
and gets ACKed; it just sits unconsumed as `ctx->receiver_ready = 1` on the
sender until `wait_for_receiver_ready_with_timeout` runs before file 2 — where
the predicate is already true, so the sender proceeds *without actually
blocking*, even though the "real" file-2 READY hasn't been sent yet. Harmless
here (the receiver's data threads have been listening since Phase 0), but the
file-2 gate ends up satisfied by a stale signal, not a fresh one.

Barrier 1 fires locally on each machine right after this — no wire bytes; the
3 idle threads on each side were parked here waiting on thread 0.

### Phase 2 — file 1's chunk stream (32 items, self-balancing)

Assuming equal-speed threads for this worked example (real runs are uneven —
A2), a round-robin claim over the shared queue gives:

| Thread (port) | Items claimed | Bytes |
|---|---|---|
| 0 (5303) | 1, 5, 9, …, 29 (8 × 16 MiB) | 128 MiB |
| 1 (5304) | 2, 6, 10, …, 30 (8 × 16 MiB) | 128 MiB |
| 2 (5305) | 3, 7, 11, …, 31 (8 × 16 MiB) | 128 MiB |
| 3 (5306) | 4, 8, 12, …, 32 (7 × 16 MiB + the 4 MiB remainder) | 116 MiB |

On each socket, independently and concurrently:

```
[data i]  Sender → Receiver  16 B  CHUNK-HDR  offset=<off>  size=<sz>   (both htobe64)
[data i]  Sender → Receiver  <sz> B  raw file payload, in ≤2 MB send() slices
             ... repeat for every item that thread claimed ...
[data i]  Sender → Receiver  16 B  CHUNK-HDR  offset=0  size=0          (terminator)
```

Concretely, thread 0's socket carries:

```
[data 0]  CHUNK-HDR offset=0          size=16777216
[data 0]  16777216 B payload
[data 0]  CHUNK-HDR offset=67108864   size=16777216      (item 5)
[data 0]  16777216 B payload
             ... 6 more 16 MiB items ...
[data 0]  CHUNK-HDR offset=0 size=0                       ← terminator, ends file 1 on this socket
```

There's no ordering *between* data ports — only within one. Once all 4
terminators are sent and received, barrier 2 fires locally on both sides,
thread 0 on each side aggregates stats ("FILE 1 COMPLETE: 500.00 MB..."), then
barrier 3 fires (also local).

**Overhead check:** header (36 B) + 32 chunk headers (512 B) + 4 terminators
(64 B) = 612 B of framing against 524,288,000 B of payload — about 1 byte of
overhead per 857,000 bytes sent, matching B3's "a millionth of the payload."

### Phase 3 — the file 1 → file 2 handshake

```
[control]  Receiver → Sender   1 B   0xAA  READY   (this is the "real" gate for file 2)
[control]  Sender   → Receiver 1 B   0xAB  ACK
```

The receiver sends this the moment its thread 0 loops back to the top for
`file_num = 2`, *before* calling `receive_file_header` again. The sender's
listener ACKs it and signals the condvar; sender thread 0 — parked in
`wait_for_receiver_ready_with_timeout` since finishing file 1 — wakes and
proceeds.

### Phases 4–5 — files 2 and 3

Identical to Phase 1–2, except:
- The header's `file_num` field is `2`, then `3`.
- File 2's header is preceded by Phase 3's READY/ACK; file 3's is preceded by
  the same handshake keyed to `file_num = 3`.
- The chunk queue is fully re-initialized per file (`init_adaptive_context`),
  so files 2 and 3 each redistribute all 32 items across the 4 threads from
  scratch — a thread that fell behind on file 1 isn't behind on file 2.
- The TCP connections themselves are never closed or reopened between files
  (A6) — only the application-level header/chunk/terminator framing repeats
  on top of them.

### Phase 6 — end of run

After file 3's barrier 3, every worker thread's loop condition
(`file_num <= ctx->max_files`) is false, so all threads simply fall out of
their loops. There is no explicit "end of session" message on any socket — the
four data connections and the one control connection are closed during
cleanup, and that's the last thing to happen on the wire.

### Whole-run tally

| Channel | Messages | Bytes |
|---|---|---|
| Control (5403) | 3 × (READY + ACK) = 6 messages | 6 |
| Data — headers | 3 | 108 |
| Data — chunk headers | 3 files × 32 items | 1,536 |
| Data — terminators | 3 files × 4 threads | 192 |
| Data — payload | 3 × 524,288,000 | 1,572,864,000 |

Framing overhead across the whole 3-repeat run: 1,536 + 192 + 108 = 1,836 bytes
against ~1.57 GB of payload — roughly 0.0001%.

---

## A. Throughput problems

### A1. One TCP connection cannot saturate a 10 GbE link

**Problem.** Several bottlenecks are per-connection or per-core: 
1. one `recv` loop runs on one core, and at 10 Gb/s the per-byte cost of copying + checksumming + syscalls can exceed what a single core delivers; 
2. modern NICs hash flows (RSS) onto multiple hardware queues, each with its own interrupt/core — one flow
engages one queue; 
3. on packet loss a single TCP flow halves its congestion window, halving the whole transfer; 
4. one retransmission stalls all in-order delivery.

**Action.** The transfer is split across **N parallel TCP streams** (default 4),
one per thread, on ports `base_port + i`. Each stream carries its own chunks
independently.

**Result.** N cores work in parallel, N NIC queues are engaged, a loss event
halves only 1/N of the aggregate, and a retransmit stalls only one stream. This
is the same reason GridFTP and every parallel-transfer tool uses 4–16 streams;
diminishing returns set in when streams exceed usable cores or start competing
for the same queues — 4–8 is the sweet spot the defaults reflect.

### A2. Static partitioning wastes time on stragglers

**Problem.** The simple tier assigns thread *i* a fixed byte range
*i·(size/N)*. Total transfer time then equals the *slowest* thread's time: if one
core is descheduled by the OS, shares a hyperthread with an interrupt handler, or
its connection hits a retransmit, its fixed share finishes late while the other
threads sit idle.

**Worked example.** Send 500 MB over 4 threads, statically partitioned: each
thread is locked in for 125 MB, decided before the transfer starts. If thread 2
hits a retransmit and slows to half speed, it alone determines the finish time —
threads 0, 1, 3 finish their 125 MB and then sit idle waiting on thread 2's
share, even though they have spare capacity to help. Four cores bought the
throughput of one slow one.

**Action.** `init_adaptive_context` slices the file into fixed **16 MB work
items** in a shared queue instead of fixed per-thread ranges — for 500 MB that's
~32 items (500 / 16 ≈ 31.25, last item ~4 MB):

```c
while (offset < file_size && work_item < MAX_WORK_ITEMS) {
    work_queue[i] = { offset, min(16MB, remaining), assigned_thread = -1 };
}
```

Threads don't own a range; they pull one item at a time under a mutex
(`get_next_work_item`), advancing a shared `work_head` index — a **task-pool /
work-queue** model. In the worked example: thread 2 stalling on one 16 MB item
costs it just that item, not a quarter of the file — threads 0, 1, 3 simply pull
more items off the queue while it's stuck, and thread 2 rejoins for whatever's
left when it recovers.

**Reassembly order.** Because items are pulled out of order and each goes down
its own thread's socket, the receiver can't assume "bytes arrive in file order."
That's solved on the wire, not here: every chunk is preceded by a 16-byte
`(offset, size)` header (B3/D1), so the receiver always knows exactly where a
chunk belongs regardless of which thread sent it or when it arrived — reassembly
is "every chunk carries its own destination address," not "trust arrival order."

**The mutex.** `get_next_work_item` is a read-modify-write on shared state: read
`work_head`, check the item's `assigned_thread`, mark it claimed, advance
`work_head`. Without a lock, two threads could both read `work_head == 5`
simultaneously, both conclude item 5 is free, and both send it — duplicate work
and a torn queue. The mutex makes that claim atomic so each 16 MB item goes to
exactly one thread.

**The grain.** "Grain" is the size of one unit of work handed out per lock
acquisition — 16 MB here. It's the dial in a granularity trade-off: too fine
(e.g. 64 KB items) and the lock is acquired so often it becomes the bottleneck
itself; too coarse (e.g. 125 MB items) and you're back to static partitioning,
losing the self-balancing property. 16 MB sits in the sweet spot — at 10 Gb/s,
sending one item takes ~13 ms, so a lock/unlock (tens of nanoseconds) is
negligible next to it, giving ~75 lock operations per second total across all
threads for the whole transfer, while a straggler still only loses one item's
worth of head start rather than a quarter of the file.

**Result.** Work assignment is *self-balancing*: fast threads simply come back
for more items, so the load distribution automatically matches each thread's
momentary speed, at a mutex cost that's negligible by design. (The lock-free
`_Atomic` counter of the simple tier would also work; the mutex version was
likely chosen because `update_thread_performance` mutates several fields that
must stay consistent anyway.)

### A3. Reading the file into a buffer doubles memory traffic

**Problem.** The obvious `read()`-into-`malloc`ed-buffer approach copies every
byte twice (page cache → user buffer, then user buffer → socket buffer), keeps a
second multi-GB replica of the file in RAM, and can't start sending until the
read finishes.

**Action.** `load_file` **`mmap`s** the entire file `PROT_READ, MAP_PRIVATE` and
workers `send()` directly out of the mapping in ≤ 2 MB slices.

**Result.** One copy instead of two (`send` copies straight from the
page-cache-backed mapping into socket buffers); pages fault in lazily with kernel
readahead streaming them, so sending starts before the whole file is "loaded";
and repeats are free — after pass 1 every page is cache-hot, so passes 2..N
measure pure network speed, exactly what a repeated-transfer benchmark wants.
(True zero-copy — `sendfile`/`MSG_ZEROCOPY`, avoiding even the page-cache→socket
copy — lives in the *simple* sender tier; this variant stops at mmap+send.
Despite the `-z` flag's name, both of its settings here go through plain `send`;
`1` merely treats partial sends as progress instead of calling `send_all`.)

### A4. Default socket buffers may stall the pipe

**Problem.** TCP can keep at most `min(cwnd, rwnd)` bytes in flight, and the
receiver's advertised window is bounded by its receive buffer. Buffers below the
**bandwidth-delay product** (`BDP = bandwidth × RTT`) cap throughput below link
rate; buffers barely at BDP leave no headroom for RTT inflation under load.

**Action.** Data sockets request **16 MB** send/receive buffers
(`SO_SNDBUF`/`SO_RCVBUF`).

**Result.** On this link RTT ≈ 100 µs, so BDP ≈ 10⁹ B/s · 10⁻⁴ s ≈ 125 KB —
16 MB is generous headroom that (a) covers queueing in the NIC and qdisc under
load, and (b) decouples the application from the network: a sender thread can
dump a full slice into the kernel and go compute its metrics while TCP drains it.
(The control socket gets the *opposite* treatment — see C2.)

### A5. Thread migration cold-caches buffers and detaches threads from NIC queues

**Problem.** Linux's scheduler migrates threads between cores by default. Each
migration invalidates warm L1/L2 contents (including the 2 MB buffer reused every
iteration) and can move a thread away from the core handling its flow's NIC
interrupt queue. It also adds run-to-run variance to a benchmark.

**Action.** `set_thread_affinity` pins thread *i* to core `i % ncpus` via
`pthread_setaffinity_np`.

**Result.** Stable cache residency, stable NUMA locality, reproducible numbers.
(The ideal setup additionally aligns each pinned thread with the IRQ affinity of
the NIC RX/TX queue carrying its flow — Receive Flow Steering territory; this
code does the thread half of that.)

### A6. Reconnecting per file wastes round-trips and ports

**Problem.** A design that opens fresh connections for every file pays, per
file: a TCP three-way handshake (1 RTT), a slow-start ramp (several RTTs before
cwnd reaches link capacity), TIME_WAIT accumulation on the closing side
(thousands of transfers → port-exhaustion risk), and re-`accept` scheduling
races.

**Action.** All N data connections are **persistent**: opened once at startup,
reused for every file. The per-file boundary is marked in-band (per-stream
`size == 0` terminator headers, B3) and by the control channel (C1), not by
closing sockets.

**Result.** All reconnection costs amortize to zero, and the congestion window
stays warm across files — which is why FILE 2..N routinely clock faster than
FILE 1.

### A7. Noisy per-chunk throughput samples are useless raw

**Problem.** To adapt scheduling to thread speed you need a per-thread
throughput estimate, but instantaneous per-chunk throughput is noisy: TCP's
congestion window ramps, the scheduler preempts threads, kernel buffers drain in
bursts. A plain average over all history reacts too slowly to genuine changes;
the raw last sample reacts too fast to noise.

**Action.** After each chunk, `update_thread_performance` folds the sample into
an **exponential moving average**:

```c
perf->throughput_mbps = 0.3 * current + 0.7 * perf->throughput_mbps;
```

and derives a **performance factor** per thread:
`factor_i = throughput_i / mean(throughput of all active threads)` (1.0 =
average, 1.3 = 30 % faster than the pack), printed per file
(`Adaptive Summary - T0:98% T1:104% ...`).

**Result.** The EMA `S ← α·x + (1−α)·S` is a one-value low-pass filter: each
sample's weight decays geometrically with age (a sample *k* chunks old
contributes `α(1−α)ᵏ`); with α = 0.3 the estimate's "memory" is roughly the last
1/α ≈ 3–4 chunks. It costs O(1) memory — no sample window — which is why the
same idea is ubiquitous in networking (TCP's own `srtt` RTT estimator is an EMA
with α = 1/8). **WIP honesty:** in this snapshot the factors are measured and
printed but **never used** — `ADAPTIVE_CHUNK_SIZE` is a compile-time constant and
nothing resizes chunks yet. The *load balancing* is real (fast threads pull more
items); the *dynamic chunk sizing* promised by the name is scaffolding awaiting
implementation (see E).

### A8. Wall clocks can jump mid-measurement

**Problem.** `CLOCK_REALTIME` can be stepped by NTP or the operator mid-run,
producing negative or nonsense "durations."

**Action.** `get_time` uses `CLOCK_MONOTONIC` (seconds since boot, immune to
clock steps) for all measurements. The one deliberate exception is
`pthread_cond_timedwait` deadlines, because POSIX condition variables default to
the realtime clock. The per-file figure uses the *union* of all threads' active
intervals — `Gbps = bytes × 8 / ((max_end − min_start) × 10⁹)`.

**Result.** Durations are monotone and comparable across runs, and any straggler
tail is correctly charged to the file's wall-clock time.

---

## B. Correctness problems

### B1. `send`/`recv` legally do partial I/O

**Problem.** POSIX stream sockets guarantee neither that `send` accepts the
whole buffer (it takes what fits in the send buffer) nor that `recv` returns the
amount asked for — it returns whatever is currently in the kernel receive
buffer. Code that assumes full transfers corrupts framing the first time the
buffer is full. Additionally, writing to a peer-closed connection raises
`SIGPIPE`, which kills the process by default; and signals (`SIGINT` is used for
shutdown) interrupt syscalls with `EINTR`.

**Action.** Every transfer goes through `send_all`/`recv_all` wrappers that
resume at `buf + total` until `len` is satisfied, treating: `recv` returning 0 =
orderly peer close (an error here — mid-message close is unexpected); `EINTR` =
retry; `ECONNRESET` = hard failure. Every `send` passes `MSG_NOSIGNAL`.

**Result.** Framing survives short reads/writes, shutdown signals don't corrupt
transfers, and a dead peer surfaces as a handleable `EPIPE` errno instead of a
fatal signal.

### B2. Two hosts may disagree on integer byte order

**Problem.** A multi-byte integer is stored little-endian on x86, big-endian
elsewhere; two hosts exchanging raw struct bytes would misread each other's
integers if their endianness differed.

**Action.** Every wire-visible integer is converted to **network byte order**
(big-endian) with `htonl`/`htobe64` on send and `ntohl`/`be64toh` on receive.

**Result.** The wire format is well-defined and portable. On this x86-to-x86
link the conversions are symmetric no-ops in effect, but writing them documents
*which* fields are wire-visible and keeps the code correct on any future
hardware.

### B3. A byte stream has no message boundaries — desync corrupts silently

**Problem.** TCP is a *byte stream*, not a message stream. If sender and
receiver ever disagree about how many bytes belong to the previous file (an
off-by-one, a dropped chunk, a stale byte in a kernel buffer), the receiver's
next read of "the header" actually consumes file *data* — and everything after
it is silently garbage. Worse, garbage parsed as a length field would drive a
multi-gigabyte `recv_all` or a buffer overflow.

**Action.** Three layers of framing defense:

1. **Per-file header** (sent by sender thread 0, parsed by receiver thread 0,
   only on data connection 0), every field validated:

   | Field | Type | Encoding | Validation on receive |
   |---|---|---|---|
   | Sync marker | `uint64_t` | `htobe64(0xDEADBEEFCAFEBABE)` | must match exactly |
   | File number | `uint32_t` | `htonl` | must equal expected `file_num` |
   | File size | `uint64_t` | `htobe64` | `0 < size ≤ 10 GB` |
   | Stream count | `uint32_t` | `htonl` | `1..16` |
   | Filename length | `uint32_t` | `htonl` | `< 256` |
   | Filename | bytes | raw, no NUL | NUL appended by receiver |

2. **Per-chunk 16-byte wire header** — `(offset, size)` as two big-endian
   `uint64_t`s — precedes every chunk's payload; a `size == 0` header terminates
   each stream's file. The receiver validates `offset + size ≤ file_size` and
   aborts on violation. (This framing was added to fix bug D1.)

3. **Range checks as defense-in-depth** on every length field before it is used.

**Result.** The 8-byte magic makes desynchronization detectable with probability
≈ 1 − 2⁻⁶⁴ instead of silently corrupting everything after it; the monotonic
file number catches the case where framing is intact but the two sides are on
different iterations; per-chunk headers make the wire itself the authority on
how many bytes each socket carries. Overhead: 16 bytes per 16 MB — one millionth
of the payload. Note this is a *trusted-peer* protocol — no payload checksum, no
authentication; fine for a point-to-point lab link, not a hostile network (E).

### B4. Threads race the per-file state without phase ordering

**Problem.** Within each process, N worker threads share one work queue and one
statistics block, re-initialized every file by thread 0 (which alone handles the
header exchange and stats aggregation — concentrating protocol work in one
thread avoids leader election or locking the header exchange). Without ordering:
a worker could call `get_next_work_item` on *last* file's drained queue, see "no
work," and skip the file entirely; or thread 0 could aggregate stats slots that
workers are still writing.

**Action.** A `pthread_barrier_t` (count N) structures each file into
**bulk-synchronous phases** — no thread enters step *k+1* until all have
finished step *k*. The per-file loop needs exactly three:

```
 SENDER thread 0                 CONTROL                RECEIVER thread 0
 ───────────────                 ───────                ─────────────────
 (file>1) wait for READY  ◄───── READY 0xAA ◄────────── signaling thread woken by
 ACK sent by listener     ─────► ACK  0xAB ───────────►   file_processing_done
 send file header         ═════════ data port 5303 ════► receive_file_header()
 init_adaptive_context()                                 init_adaptive_context()
 ── BARRIER 1 (all sender threads) ──                    ── BARRIER 1 (all recv threads) ──
 all threads: pull 16MB work items,                      all threads: read (offset,size)
   send 16B chunk hdr + chunk bytes  ════════════════►     chunk headers + chunk bytes
   on own socket; EMA update after                         until size==0 terminator;
   each chunk; size==0 hdr when done                       EMA update after each chunk
 ── BARRIER 2 ──                                         ── BARRIER 2 ──
 thread 0: aggregate stats,                              thread 0: aggregate stats,
   files_completed++                                       files_completed++
 ── BARRIER 3 ──                                         ── BARRIER 3 ──
 loop to next file                                       loop; thread 0 signals
                                                           ready thread before the
                                                           next header read
```

1. **Barrier 1 (post-header):** workers may not pull work until thread 0 has
   parsed the header and re-initialized the shared queue.
2. **Barrier 2 (post-data):** thread 0 may not aggregate `file_bytes_*` and
   min/max timestamps until every worker has written its slot; this barrier *is*
   the "file finished" event that makes `max_end − min_start` meaningful.
3. **Barrier 3 (post-stats):** workers may not race into the next iteration (and
   thread 0 may not reset `file_processing_done`) while thread 0 still reads
   their per-file arrays.

**Result.** Each file is a clean superstep; no stale-queue skips, no torn stats.
One subtlety motivates the error-handling style throughout: `pthread_barrier_wait`
counts *arrivals*, so a thread that exits early would deadlock the rest forever.
Every error path in thread 0 therefore sets `ctx->test_active = 0` **and still
calls `pthread_barrier_wait` once** before `break`ing — it "shows up" one last
time so peers pass the barrier, observe `test_active == 0`, and exit through the
same door. Note these barriers only synchronize threads *within one process*;
the cross-machine half is problem C1 — local barrier → READY over TCP → remote
header send compose into a distributed barrier.

### B5. Threads must sleep until a flag changes — without losing wakeups

**Problem.** The signaling threads need "sleep until some state changes"
(receiver: `file_processing_done`; sender: `receiver_ready`). Busy-polling burns
a core; naive condvar use loses signals, because POSIX permits *spurious
wakeups* and a signal on a condition variable with no waiter vanishes —
condition variables are stateless.

**Action.** The textbook mutex + condition-variable pattern:

```c
pthread_mutex_lock(&m);
while (!predicate)
    pthread_cond_timedwait(&cv, &m, &deadline);
pthread_mutex_unlock(&m);
```

The mutex protects the predicate; the wait atomically releases it and sleeps,
re-acquiring on wake; the `while` re-checks the predicate every wake. Using
`timedwait` (not plain `wait`) bounds every sleep — the receiver uses its 10 s
timeout as a heartbeat opportunity (health-check the control socket), the sender
uses 30 s as a deadlock escape hatch ("PROCEEDING WITHOUT CONFIRMED READY
SIGNAL").

**Result.** Persistent state lives in the predicate variable, not in the act of
signaling, so an early signal is never lost; spurious wakeups are harmless; and
potential deadlocks degrade into timeouts + fallback behavior. (The sender
originally violated the predicate-loop rule — that was bug D3.)

### B6. Cross-thread flags without atomics

**Problem.** `running`, `test_active`, `files_completed` are polled across
threads and mutated in a signal handler; a compiler may legally cache a plain
`int` in a register forever, so the polling loop would never see the update.

**Action.** The flags are declared `volatile int`, which forbids register
caching.

**Result.** The polling loops work — but `volatile` provides **no atomicity or
memory ordering** in the C11 model; technically these should be
`_Atomic`/`sig_atomic_t`. It holds in practice on x86 (strong memory model,
aligned word stores are atomic) — a common but formally unsound idiom worth
recognizing, listed as an open problem (E).

---

## C. Coordination problems

### C1. When may the sender transmit the next file's header?

**Problem.** This is where most of the two files' complexity lives. If the
sender transmits header *k+1* while the receiver is still finishing file *k*,
the bytes sit harmlessly in socket 0's buffer — *unless* the receiver has
desynchronized, in which case they're consumed as data. The older variant
"solved" this with a `usleep(1000)` guess (still visible, commented out, at the
bottom of the sender loop) — a timing assumption, not a guarantee. And plain TCP
delivery isn't confirmation: `send()` returning success only means the byte
entered the *local* kernel buffer; the peer process may be dead, hung, or behind
a half-broken link, and the sender won't learn for minutes (TCP retransmission
timeouts back off toward ~15 min).

**Action.** An explicit application-level handshake on a dedicated control
connection:

- Receiver → Sender: `READY (0xAA)` — "all my threads passed the file boundary;
  thread 0 is about to block in `receive_file_header`."
- Sender → Receiver: `ACK (0xAB)` — application-level acknowledgment.
- `HEARTBEAT (0xBB)` is defined and echoed by the sender's listener, but the
  receiver never sends one in this snapshot.

The **receiver** initiates the connection and pushes READY because it is the
party that knows when it is back at the top of its loop. On the sender, a
dedicated `ready_signal_listener` thread accepts the connection and loops in
`select()` with a 5 s timeout — making the blocking read *interruptible on a
schedule* (notice `running == 0`, run health checks) without nonblocking-socket
spin; on READY it ACKs and performs the condvar handoff (`receiver_ready = 1;
pthread_cond_signal`), waking sender thread 0 parked in
`wait_for_receiver_ready_with_timeout` (B5).

**Result.** The sender never transmits a header before the remote *application*
has confirmed it is listening — the end-to-end argument in miniature:
reliability that matters to the application must be confirmed at the application
layer. Combined with each side's barrier 3, the exchange forms a distributed
barrier between machines.

### C2. Control signals must not queue behind bulk data

**Problem.** Piggybacking READY/ACK on a data socket would put a 1-byte signal
*behind* megabytes of queued bulk data in kernel buffers — delayed until the
bulk drains. Two further latency traps: Nagle's algorithm delays small segments
while previous data is unacknowledged (and interacts pathologically with delayed
ACKs — up to ~40–200 ms stalls), and large socket buffers turn buffered bytes
into signaling *latency*.

**Action.** The control channel is its **own TCP connection** with deliberately
**tiny 4 KB buffers** and `TCP_NODELAY` set. (`TCP_NODELAY` is also set on data
sockets: their 2 MB writes never benefit from Nagle, but the file header's
several small sequential `send_all` calls would otherwise be serialized against
ACKs.)

**Result.** A READY/ACK crosses in one RTT regardless of data-socket buffer
occupancy; a small `SO_SNDBUF` also means a failed peer is detected sooner,
since sends stop succeeding earlier.

### C3. The control channel must survive a flaky or dead peer

**Problem.** An idle TCP connection exchanges *nothing*: if the peer machine
loses power, the survivor's socket stays "ESTABLISHED" forever (Linux default
keepalive: ~2 hours). `SO_ERROR` alone misses half-closed/reset states. And
retrying a failed signal immediately adds load exactly when a struggling peer
can least absorb it.

**Action.** `send_ready_signal_bulletproof` makes up to 7 attempts, each
structured *prevention → action → confirmation*:

1. **Prevention — `comprehensive_socket_health_check`:** three probes of
   increasing depth: `SO_ERROR` (pending async error?), `TCP_INFO` state (still
   `ESTABLISHED`? catches what `SO_ERROR` misses), and a zero-byte
   `MSG_DONTWAIT` send (exercises the send path without payload). If unhealthy,
   tear down and rebuild via `connect_to_sender`.
2. **Action:** send the 1-byte READY.
3. **Confirmation:** `select()` with an 8 s timeout waiting for the ACK.

Between failed attempts it sleeps `min(2^attempt, 8)` seconds — **exponential
backoff with a cap**; the same pattern governs the initial `connect_to_sender`
(10 tries, 1→8 s). The control socket also arms **aggressive TCP keepalive**
(idle 20 s, probe every 3 s, 3 failures).

**Result.** Broken sockets are detected *before* being trusted with a signal and
rebuilt proactively; a dead peer is declared in ~29 s instead of ~2 hours;
geometric retry spacing sheds load and rides out transients (route flap, peer
restart) while the cap bounds worst-case latency.

### C4. The two processes must start in the right order — but not too strictly

**Problem.** Connections need a listener before a connector, in *both*
directions: receiver listens for data, sender listens for control. A rigid
ordering requirement makes the system fragile to start-up timing.

**Action.** The startup sequences interlock:

**Receiver `main`:** (1) create one listening socket per stream on
`base_port + i`; (2) spawn `num_streams` × `adaptive_receiver_thread`, each
blocking in `accept()`; (3) busy-poll (10 ms sleep loop) until all `client_fd`s
are set; (4) **then** connect out to the sender's control port
(`connect_to_sender`, up to 10 retries with exponential backoff) and spawn
`ready_signaling_thread`; (5) set `file_processing_done = 1` so the very first
READY fires.

**Sender `main`:** (1) `load_file()` mmap (A3); (2) spawn
`ready_signal_listener` *before* touching data connections, then `sleep(1)` so
the listener has time to `bind()`/`listen()` — a race-avoidance-by-delay pattern
(a condition variable would be deterministic, but 1 s is far longer than a bind
takes in practice); (3) connect stream *i* to receiver port `base_port + i` for
all *i*; (4) spawn `num_streams` × `adaptive_sender_thread`.

**Result.** The run instruction "start the receiver first" is the only hard
requirement (its data listeners must exist before the sender connects), while
the control connection tolerates the sender being slightly late thanks to the
receiver's retry loop.

---

## D. Bugs found in review — and their fixes

Four defects found during review of the original snapshot have since been
**fixed in the working tree**. Each is its own miniature STAR.

### D1. FIXED — independent work queues could desynchronize per-socket byte counts

**Problem.** Each side sliced the file into 16 MB items in its **own** queue,
with no per-chunk framing on the wire. Nothing forced sender thread *i* and
receiver thread *i* to claim the *same number* of items: if sender threads
claimed (5, 3, 4, 4) items but receiver threads expected (4, 4, 4, 4), one
receiver thread would block forever waiting for bytes that went down a different
socket. The balance was statistical, not guaranteed.

**Action.** The sender now precedes every chunk with the 16-byte
`(offset, size)` header and ends each stream's file with a `size == 0`
terminator (B3). The receiver's loop is driven entirely by these headers — its
own work queue is no longer consulted for receiving (its queue-init code remains
only to set up shared performance-tracking state).

**Result.** The wire is the single source of truth for per-socket byte counts —
no hang is possible from asymmetric item claims — and the transmitted offsets
are the hook for future disk-write/reassembly support.

### D2. FIXED — hardcoded control-channel constants

**Problem.** The sender's listener hardcoded port 5403, and the receiver's
reconnect path hardcoded `"169.254.207.123"` and 5403 — any non-default
`base_port` or sender IP silently broke the control channel.

**Action.** Both sides now derive the control port as `base_port + 100` (stored
in their contexts), and the receiver's reconnect uses the `sender_ip` from argv.

**Result.** Any base port works as long as both sides agree; the pair is
deployable on other addresses.

### D3. FIXED — lost-wakeup race in the sender's ready wait

**Problem.** `wait_for_receiver_ready_with_timeout` called
`pthread_cond_timedwait` **without first checking `ctx->receiver_ready`** —
violating the predicate rule from B5. If the READY byte arrived before sender
thread 0 reached the wait (likely: the receiver signals right after its
barrier 3 while sender thread 0 is still printing stats), the signal landed on
an empty wait queue and was lost; thread 0 then burned the full 30 s timeout and
proceeded "without confirmed ready."

**Action.** The function now uses the textbook predicate loop
(`while (!ctx->receiver_ready) timedwait(...)`).

**Result.** An early READY is seen immediately; no 30 s stalls between files.
This bug doubles as the worked example for the condition-variable theory in B5.

### D4. FIXED — loop bound used the macro, not the CLI argument

**Problem.** Worker loops on both sides iterated to the compile-time
`MAX_FILES` (10) while `main` waited on the CLI `max_files`; passing
`max_files > 10` hung the run.

**Action.** The worker loops, the receiver's `ready_signaling_thread`, and the
progress printfs now all use `ctx->max_files`. (The `performance_factor` update
loop iterating only the first `active_threads` slots was fixed at the same time
— it now scans all 32 slots and filters on `chunks_completed`.)

**Result.** `max_files` may be any value; the CLI argument is honored end to
end.

---

## E. Open problems — not yet solved

- **Receiver discards data** (no disk write, no reassembly) — this variant
  benchmarks the network path only. The `_SocketForSynchronization` / non-WIP
  receiver variants are where file output lives. The chunk offsets now arriving
  on the wire (D1) are the hook for adding it back.
- **Chunk size is still fixed** at 16 MB; performance factors are measured but
  not used to resize anything (A7). The intended endgame: bigger chunks to fast
  threads (fewer queue trips), smaller chunks to slow ones (finer-grained
  stealing near end-of-file).
- `init_adaptive_context` `memset`s a struct containing an **initialized mutex**
  on every file (and re-inits it) — formally undefined if any thread could still
  hold it; safe here only because barriers guarantee quiescence.
- File sizes above `MAX_WORK_ITEMS × 16 MB = 16 GB` would silently truncate the
  sender's queue (though the 10 GB header sanity cap is hit first).
- **No payload checksum or authentication** — trusted point-to-point link
  assumed (B3).
- The `volatile`-instead-of-`_Atomic` flags (B6).

---

## Build and run

```bash
# Sender (RDB)
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_sender file_sender_rdb_controlled_adaptive_v3_WIP.c -lm

# Receiver (HPC)
gcc -O3 -Wall -pthread -std=gnu11 -o adaptive_receiver file_receiver_hpc_controlled_adaptive_v3_WIP.c -lm
```

```bash
# 1. Start the receiver first (its data listeners must exist — C4):
./adaptive_receiver 5303 4 10 169.254.207.123
#                   port streams max_files sender_ip

# 2. Then the sender (its control listener starts before data connects):
./adaptive_sender 169.254.207.40 /path/to/file.png 4 1 5303 10
#                 receiver_ip    file             streams zerocopy port max_files
```

The control channel always uses `base_port + 100` on both sides, so any base
port works as long as sender and receiver agree (D2); `max_files` may be any
value (D4).

---

## Quick reference: problem → solution, one line each

| Problem | Solution | Section |
|---|---|---|
| One flow / one core can't fill 10 Gb/s | N parallel TCP streams across N cores and NIC queues | A1 |
| Stragglers idle the other threads | Shared work queue — fast threads pull more items | A2 |
| Queue lock could bottleneck | Large 16 MB grain → ~75 locks/s total | A2 |
| Double-copy of a multi-GB file | `mmap` + send straight from page cache | A3 |
| Buffers below BDP cap throughput | 16 MB data-socket buffers (BDP ≈ 125 KB + headroom) | A4 |
| Thread migration cold-caches buffers | CPU affinity: thread *i* → core `i % ncpus` | A5 |
| Per-file reconnect overhead | Persistent connections; warm cwnd across files | A6 |
| Noisy throughput samples | EMA (α = 0.3): O(1) low-pass filter | A7 |
| Wall clock jumps mid-benchmark | `CLOCK_MONOTONIC` durations | A8 |
| Partial `send`/`recv` corrupt framing | `send_all`/`recv_all` loops, `EINTR` retry, `MSG_NOSIGNAL` | B1 |
| Endianness mismatch on the wire | Network byte order for every wire integer | B2 |
| Byte-stream desync corrupts silently | Magic number + file counter + validated per-chunk headers | B3 |
| Workers race per-file shared state | Three-barrier bulk-synchronous loop; error paths still "arrive" | B4 |
| Sleeping on a flag loses wakeups | Mutex + condvar with predicate loop and bounded `timedwait` | B5 |
| Compiler may cache cross-thread flags | `volatile` (works on x86; `_Atomic` is the correct fix) | B6 |
| When to send the next header? | READY/ACK app-level handshake — end-to-end confirmation | C1 |
| Signals queue behind bulk data | Separate control connection, 4 KB buffers, `TCP_NODELAY` | C2 |
| Dead peer looks ESTABLISHED for hours | Health checks + aggressive keepalive + capped exponential backoff | C3 |
| Startup ordering races | Receiver-first data listeners; retried control connect | C4 |
| Asymmetric queue claims hung a thread | Wire-driven receiving via per-chunk headers (FIXED) | D1 |
| Hardcoded port/IP broke non-defaults | Derive `base_port + 100`; use argv `sender_ip` (FIXED) | D2 |
| Early READY was lost, 30 s stall | Predicate loop before `timedwait` (FIXED) | D3 |
| `max_files > 10` hung the run | Loops honor CLI `ctx->max_files` (FIXED) | D4 |
