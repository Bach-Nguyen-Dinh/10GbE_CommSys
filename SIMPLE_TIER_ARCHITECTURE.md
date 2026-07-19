# Simple Tier Sender/Receiver — Problems and How They Were Solved

This document explains how `simple_sender_rdb.c` (runs on **RDB**,
`169.254.207.123`) and `simple_receiver_hpc.c` (runs on **HPC**,
`169.254.207.40`) cooperate to move one file repeatedly over a 10 GbE link. It is
structured like a STAR interview answer: each section states a **Problem** (the
situation and what had to be achieved), the **Action** taken in the code, and the
**Result** — what that bought, with the theory behind it. It assumes you can read
C but not that you know POSIX networking or threading internals.

It is the companion to [ADAPTIVE_V3_WIP_ARCHITECTURE.md](ADAPTIVE_V3_WIP_ARCHITECTURE.md),
which documents the adaptive tier. Where the two tiers make *different* choices
for the same problem, this document says so explicitly — the contrast is where
most of the insight lives.

> **Important framing:** this tier is the **baseline**, and it is deliberately
> simpler than the adaptive tier in three structural ways: (1) it transfers **one
> file**, repeated *R* times, rather than a stream of files; (2) it opens **fresh
> data connections for every repeat** instead of persisting them; (3) its
> distinguishing feature is not adaptivity but **three selectable kernel transmit
> paths** — `writev`, `sendfile`, and `MSG_ZEROCOPY` — which is what makes it the
> tier you use to answer "which zero-copy mechanism is actually fastest on this
> hardware?" Unlike the adaptive WIP receiver, this receiver **can** write the
> file to disk (`save_to_disk = 1`), and also has a discard mode for pure
> benchmarking.

---

## The situation: what the system has to do

Move one file from RDB to HPC over a back-to-back 10 GbE link, *R* times in a
row, measuring throughput per repeat — as fast as the link allows, optionally
reassembling and saving the file on the receiving side, and reporting live
progress to a web UI.

```
        RDB (sender)                                HPC (receiver)
  ┌──────────────────────┐                    ┌──────────────────────┐
  │ main()               │                    │ main()               │
  │  load file (or open) │                    │  listener on 5303    │
  │                      │   CONTROL (TCP)    │  + listeners 5304-07 │
  │  connect ── 5303 ──────────────────────►  │  accept() control    │
  │  send file_header_t  ═════ 280 B ═══════► │  validate magic      │
  │  wait for 'R'  ◄────────── 1 B ◄───────── │  send_ready_signal   │
  │                      │                    │                      │
  │  ┌─ per repeat ────┐ │    DATA (TCP)      │ ┌─ per repeat ─────┐ │
  │  │ build work queue│ │                    │ │ spawn N threads  │ │
  │  │ connect N socks │ │ ================>  │ │ each accept()s   │ │
  │  │ spawn N threads │ │                    │ │ START BARRIER    │ │
  │  │ thread 0 ─ 5304 ──────────────────────►│ │ thread 0         │ │
  │  │ thread 1 ─ 5305 ──────────────────────►│ │ thread 1         │ │
  │  │ thread 2 ─ 5306 ──────────────────────►│ │ thread 2         │ │
  │  │ thread 3 ─ 5307 ──────────────────────►│ │ thread 3         │ │
  │  │ join; close all │ │  (close = EOF)     │ │ END BARRIER (N+1)│ │
  │  │ wait for 'R' ◄──┼─┼───── 1 B ◄─────────┼─┤ send_ready_signal│ │
  │  └─────────────────┘ │                    │ └──────────────────┘ │
  │                      │                    │  save_file()         │
  └──────────────────────┘                    └──────────────────────┘
                                                       │ UDP 5666
                                                       ▼
                                              Flask web UI (progress)
```

| Channel | Who listens | Who connects | Ports | Lifetime | Purpose |
|---|---|---|---|---|---|
| **Control** (1 connection) | Receiver (HPC) | Sender (RDB) | `base_port` (5303) | Whole run | One `file_header_t` at startup; then one `'R'` ready byte per repeat, receiver → sender |
| **Data** (N connections) | Receiver (HPC) | Sender (RDB) | `base_port + 1 + i` (5304..5307) | **One repeat** | Bulk file bytes as `(24 B chunk header + payload)` pairs; socket close = end of stream |
| **Progress** (UDP, one-way) | Web UI (`127.0.0.1`) | Receiver | 5666 | Whole run | `PROGRESS:` / `TRANSFER_START:` / `TRANSFER_COMPLETE:` text datagrams |

Note the control channel here is the **base port itself**, and it is the *first*
connection made — the opposite of the adaptive tier, where the receiver dials out
to a separate control port `base_port + 100` on the sender. Consequently the data
ports start at `base_port + 1`, not `base_port`.

The problems fall into five groups, plus one worked trace of the literal bytes on
the wire:

- **[A. Throughput](#a-throughput-problems)** — why a simple `read()`/`send()` loop can't fill 10 Gb/s.
- **[B. Correctness over a byte stream](#b-correctness-problems)** — why TCP alone doesn't keep two programs in agreement.
- **[C. Coordination between repeats](#c-coordination-problems)** — when may the sender start the next repeat?
- **[D. Defects found in review](#d-defects-found-in-review)** — bugs present in the current snapshot, with remedies.
- **[E. Open problems](#e-open-problems--not-yet-solved)** — what is genuinely unfinished or out of scope.
- **[Wire sequence, worked example](#wire-sequence--worked-example-500-mb--3-repeats-4-streams-32-mb-chunks)** — every byte, in order, for a concrete 500 MB × 3 run.

---

## Wire sequence — worked example: 500 MB × 3 repeats, 4 streams, 32 MB chunks

Sections A–E explain *why* each mechanism exists. This section answers a narrower
question: for one concrete run, **what actually crosses the wire, in what order,
with what bytes**.

```bash
./simple_receiver_hpc 5303 4 1 ./received
#                     port streams save_to_disk output_dir
./simple_sender_rdb 169.254.207.40 demo.bin -s 4 -z 1 -p 5303 -r 3
```

500 MB file (524,288,000 bytes, `demo.bin`), 4 streams (control 5303, data
5304–5307), 32 MiB chunks → the file splits into **16 work items**: 15 full
33,554,432 B items + one 20,971,520 B (20 MiB) remainder
(524,288,000 = 15 × 33,554,432 + 20,971,520). `-r 3`, so the whole thing repeats
three times over three *separate* sets of data connections.

### Phase 0 — control setup (once for the whole run)

| # | Who | Action | Wire content |
|---|---|---|---|
| 1 | Receiver | opens UDP progress socket to `127.0.0.1:5666`; binds+listens on 5303; blocks in `accept()` | — |
| 2 | Sender | `-z 1`, so it `stat()`s the file for its size and `open()`s it read-only (no `malloc`, no read) | — |
| 3 | Sender | connects to 5303 with `TCP_NODELAY` and a 4 MB `SO_SNDBUF` | TCP handshake only |
| 4 | Receiver | `accept()` returns; then `create_listener()` for 5304, 5305, 5306, 5307 (128 MB `SO_RCVBUF` each, backlog 128) | — |

### Phase 1 — the first payload bytes of the run

```
[control]  Sender → Receiver  280 B   file_header_t, native little-endian, raw struct:
             0..7    magic        0xDEADBEEFCAFEBABE
             8..15   file_size    524288000
             16..271 filename[256] "demo.bin\0" + 247 unused bytes
             272..275 repeat_count 3
             276..279 <tail padding, uninitialized>          ← see D2
[control]  Receiver → Sender    1 B   0x52  'R'   READY
```

The receiver validates `magic` only. It then `malloc`s the **entire 500 MB**
reassembly buffer up front (`save_to_disk = 1`), allocates its barriers
(`start_barrier` = 4, `end_barrier` = 5), and sends the initial `'R'`.

**Note the asymmetry with the adaptive tier:** this header is sent **once for the
whole run**, not once per file. It carries `repeat_count`, so the receiver knows
in advance how many times to loop. There is no per-repeat header and no
per-repeat sync marker — the *only* magic number on the wire is this one, at
second zero.

### Phase 2 — repeat 1's data connections

```
[TCP]  Sender → Receiver  connect 5304, 5305, 5306, 5307 (in that order,
                          each with TCP_NODELAY + 128 MB SO_SNDBUF requested)
```

Each connection is accepted by a freshly spawned receiver thread, which sets
`TCP_NODELAY` and a 128 MB `SO_RCVBUF` on the accepted socket, then parks at
`start_barrier`. When all four arrive, the barrier releases and thread 0 fires
`TRANSFER_START:1:current_file.png:<t>` over UDP to the web UI.

### Phase 3 — repeat 1's chunk stream (16 items, self-balancing)

Threads pull items from a lock-free queue with `atomic_fetch_add`, so assignment
is by *arrival at the counter*, not by static range. Assuming equal-speed threads
for this example:

| Thread (port) | Items claimed | Bytes |
|---|---|---|
| 0 (5304) | 0, 4, 8, 12 | 128 MiB |
| 1 (5305) | 1, 5, 9, 13 | 128 MiB |
| 2 (5306) | 2, 6, 10, 14 | 128 MiB |
| 3 (5307) | 3, 7, 11, 15 (last is the 20 MiB remainder) | 116 MiB |

On each socket, independently and concurrently:

```
[data i]  Sender → Receiver   24 B  chunk_header_t, native little-endian:
             0..3    chunk_id     e.g. 4
             4..7    <hole, uninitialized>                   ← see D2
             8..15   file_offset  134217728
             16..19  chunk_size   33554432
             20..23  padding      0
[data i]  Sender → Receiver  33554432 B  raw file payload
             ... repeat for every item that thread claimed ...
[data i]  Sender → Receiver  FIN   (close(socket_fd) — this is the end-of-stream marker)
```

Concretely, thread 0's socket carries:

```
[data 0]  CHUNK-HDR id=0   offset=0          size=33554432
[data 0]  33554432 B payload
[data 0]  CHUNK-HDR id=4   offset=134217728  size=33554432
[data 0]  33554432 B payload
[data 0]  CHUNK-HDR id=8   offset=268435456  size=33554432
[data 0]  33554432 B payload
[data 0]  CHUNK-HDR id=12  offset=402653184  size=33554432
[data 0]  33554432 B payload
[data 0]  FIN                                        ← ends the stream; no terminator record
```

**There is no `size == 0` terminator here** — that is an adaptive-tier
construct. In the simple tier the sender's `close()` is the framing: the
receiver's `recv()` for the next chunk header returns 0, which it treats as
"normal end of transfer" and exits its loop. This is *why* the data connections
cannot be persistent (A6/C2).

While this runs, each receiver thread emits a UDP datagram every 16 MB or 0.5 s,
whichever comes first:

```
[udp]  Receiver → 127.0.0.1:5666   "PROGRESS:1:0:134217728:524288000:4:12345.678"
                                    ^repeat ^tid ^bytes ^total ^chunks ^monotonic ts
```

**Overhead check:** 16 chunk headers × 24 B = 384 B of framing against
524,288,000 B of payload — about 1 byte of overhead per 1.37 million bytes sent.

### Phase 4 — the repeat 1 → repeat 2 handshake

```
[udp]      Receiver → UI          "TRANSFER_COMPLETE:1:current_file.png:<t>"   (thread 0)
[TCP]      all four data connections fully closed on both sides
[control]  Receiver → Sender  1 B   0x52  'R'
```

The receiver's threads each hit `end_barrier` (count 5) after closing their
socket; `main` is the fifth arrival, so it unblocks, joins all four threads, sums
the per-thread metrics, and only then sends `'R'`. The sender — parked in
`wait_for_ready_signal` on the control socket — wakes and begins repeat 2.

### Phases 5–6 — repeats 2 and 3

Identical to Phases 2–3, except:

- **Four brand-new TCP connections** are established each time (A6). Across the
  whole run that is 12 data connections plus 1 control connection.
- The work queue is rebuilt from scratch (`create_work_queue` / `cleanup_work_queue`
  per repeat), so item→thread assignment is redecided every repeat.
- Nothing on the wire distinguishes repeat 2 from repeat 1 — no counter, no
  magic. The *connection boundary itself* is the repeat boundary.
- After the **last** repeat, no `'R'` is sent and none is awaited.

### Phase 7 — end of run

The sender closes the control socket and the file descriptor and prints its
`=== FINAL AVERAGE RESULTS ===` block (the one the RDB Flask service parses). The
receiver writes the reassembled 500 MB buffer to `./received/demo.bin` with a
single `fwrite`, prints its own averages, and tears down.

### Whole-run tally

| Channel | Messages | Bytes |
|---|---|---|
| Control — file header | 1 | 280 |
| Control — ready signals | 3 (initial + 2 inter-repeat) | 3 |
| Data — chunk headers | 3 repeats × 16 items | 1,152 |
| Data — payload | 3 × 524,288,000 | 1,572,864,000 |
| Data — TCP connections | 3 repeats × 4 streams | 12 handshakes + 12 teardowns |
| UDP progress (off-link, loopback) | ~100+ datagrams | negligible |

Framing overhead across the whole 3-repeat run: 1,152 + 280 + 3 = 1,435 bytes
against ~1.57 GB of payload. The *real* per-repeat cost in this tier is not
framing bytes — it is the 4 TCP handshakes and slow-start ramps per repeat (A6).

---

## A. Throughput problems

### A1. One TCP connection cannot saturate a 10 GbE link

**Problem.** Several bottlenecks are per-connection or per-core:
1. one `send`/`recv` loop runs on one core, and at 10 Gb/s the per-byte cost of copying + checksumming + syscalls can exceed what a single core delivers;
2. modern NICs hash flows (RSS) onto multiple hardware queues, each with its own interrupt/core — one flow engages one queue;
3. on packet loss a single TCP flow halves its congestion window, halving the whole transfer;
4. one retransmission stalls all in-order delivery.

**Action.** The transfer is split across **N parallel TCP streams** (default 4),
one per thread, on ports `base_port + 1 + i`. Each stream carries its own chunks
independently.

**Result.** N cores work in parallel, N NIC queues are engaged, a loss event
halves only 1/N of the aggregate, and a retransmit stalls only one stream. This
is the same reason GridFTP and every parallel-transfer tool uses 4–16 streams;
diminishing returns set in when streams exceed usable cores or start competing
for the same queues — 4–8 is the sweet spot the defaults reflect. (In this tier
there is an additional, artificial reason not to exceed 4: the hardcoded
affinity modulus — see D1.)

### A2. Static partitioning wastes time on stragglers — solved *lock-free* here

**Problem.** Assigning thread *i* a fixed byte range *i·(size/N)* makes total
transfer time equal the *slowest* thread's time: if one core is descheduled by the
OS, shares a hyperthread with an interrupt handler, or its connection hits a
retransmit, its fixed share finishes late while the other threads sit idle. Four
cores buy the throughput of one slow one.

**Action.** `create_work_queue` slices the file into fixed **32 MB work items** in
a shared queue. Threads pull one item at a time — but with **no mutex at all**:

```c
typedef struct {
    work_item_t *items;
    int total_items;
    _Atomic int current_item;   // lock-free
} work_queue_t;

int get_next_work_item(work_queue_t *queue, work_item_t *item) {
    int idx = atomic_fetch_add(&queue->current_item, 1);
    if (idx >= queue->total_items) return -1;
    *item = queue->items[idx];
    return 0;
}
```

**Why this is correct without a lock.** `atomic_fetch_add` is a single
read-modify-write that the hardware performs indivisibly (on x86, a `lock xadd`).
Every caller receives a *distinct* return value, so every index is handed to
exactly one thread — the property the adaptive tier needs a mutex to guarantee.
It works here because a claim is *only* "reserve the next index"; the adaptive
tier's `get_next_work_item` additionally mutates several per-item and per-thread
fields that must stay mutually consistent, which is a genuine critical section
and not expressible as one atomic instruction.

**Cost.** One uncontended `lock xadd` is tens of nanoseconds; under contention
it costs a cache-line transfer between cores. With 32 MB items at 10 Gb/s each
item takes ~27 ms to send, so the queue is touched roughly **37 times per second
in total across all threads**. The counter's cache line is stone cold between
accesses. The synchronization cost is, for practical purposes, zero.

**Reassembly order.** Because items are pulled out of order and each goes down a
different socket, the receiver cannot assume "bytes arrive in file order." That
is solved on the wire: every chunk is preceded by a 24-byte header carrying
`(chunk_id, file_offset, chunk_size)`, and the receiver writes payload directly
to `file_data + file_offset`. Reassembly is "every chunk carries its own
destination address," not "trust arrival order." **This is worth emphasizing: the
simple tier had per-chunk offset framing from the start — it is precisely what
the adaptive tier had to add later as bug fix D1.**

**The grain.** 32 MB here versus 16 MB in the adaptive tier. Grain is the dial in
a granularity trade-off: too fine and the shared counter is touched so often it
starts costing cache-line ping-pong; too coarse and you drift back toward static
partitioning, losing the self-balancing property. 32 MB is defensible given the
lock-free claim is nearly free — but it does mean that for a 500 MB file there
are only 16 items for 4 threads, i.e. **4 items each**. The load balancing has
very little room to work: one straggler costs up to 27 ms of tail, and a file
under ~128 MB gives some threads nothing at all to do. A smaller grain would cost
nothing measurable here and would balance better; 32 MB is the more questionable
of the two tiers' choices.

**Result.** Work assignment is self-balancing at effectively zero synchronization
cost — fast threads simply come back for more items. The caveat is granularity,
not mechanism.

### A3. Copying every byte through userspace is the throughput ceiling — three escape hatches

**Problem.** The obvious approach — `read()` the file into a `malloc`ed buffer,
then `send()` it — copies every byte **twice**: page cache → user buffer (at
`read` time), then user buffer → socket buffer (at `send` time). At 10 Gb/s that
is 1.25 GB/s of payload, so 2.5 GB/s of memcpy plus the page-cache reads
themselves — enough memory bandwidth and cache pollution to become the binding
constraint before the NIC does. It also keeps a second multi-GB replica of the
file in RAM.

**Action.** This is the tier's centerpiece. `-z` selects one of **three transmit
paths**, each eliminating a different amount of copying:

| `-z` | Path | Syscall | Copies | Data source |
|---|---|---|---|---|
| `0` | writev | `writev(fd, iov, 1)` | 2 (read into `malloc` buffer, then copy to socket) | user heap buffer |
| `1` | **sendfile** (default) | `sendfile(sock, file_fd, &off, n)` | **0** in userspace | page cache, directly |
| `2` | MSG_ZEROCOPY | `sendmsg(sock, &msg, MSG_ZEROCOPY)` | 1 (read into buffer), then **0** on send | user heap buffer, pinned |

**`-z 1` — `sendfile`.** The kernel splices page-cache pages straight into the
socket's send queue; the bytes never enter the process's address space at all.
`main` skips `load_file` entirely and only `stat()`s the file for its size, then
`open()`s it once. All N threads share that single `file_fd` — which is safe
*only* because `sendfile`'s offset is passed by pointer (`off_t *off`) and each
thread passes its own stack variable. The shared file description's own file
position is never read or advanced, so there is no race. Had the code used
`read()` on a shared fd instead, threads would have corrupted each other's
positions immediately.

**`-z 2` — `MSG_ZEROCOPY`.** The file *is* loaded into a heap buffer, but `send`
does not copy it: the kernel pins the user pages and the NIC DMAs from them
directly. The catch is that the buffer must not be modified until the kernel says
it is done with it, and that notification arrives asynchronously on the socket's
**error queue**. Hence `drain_zerocopy_completions()`, which does a non-blocking
`recvmsg(..., MSG_ERRQUEUE | MSG_DONTWAIT)` after each send and on `EAGAIN`.
Here the buffer is read-only for the process's entire lifetime, so the code is
safe regardless of whether it drains promptly — the drain exists to keep the
error queue from filling and stalling the socket, not to protect the data.
`SO_ZEROCOPY` is enabled per-socket at thread start, and if `setsockopt` fails
(older kernel, unsupported NIC) the thread **silently falls back to `-z 0`**.

**`-z 0` — `writev`.** The baseline. Retained deliberately as the control case in
the comparison, since the whole point of the tier is measuring what the other two
buy you. `prefetch_chunk_data` walks the chunk issuing `__builtin_prefetch` every
512 bytes to warm L1/L2 ahead of the copy, which helps only this path and `-z 2`.

**Result.** You can measure all three paths against the same file, the same
queue, the same threads, and the same link, changing one CLI flag. The expected
ordering on 10 GbE is `sendfile ≈ MSG_ZEROCOPY > writev`, with `MSG_ZEROCOPY`
sometimes losing on smaller chunks because page pinning and completion
bookkeeping have per-call overhead that only amortizes over large sends — which
is one motivation for the 32 MB grain (A2).

**Contrast with the adaptive tier:** that tier `mmap`s the file and uses plain
`send`, which is *one* copy (page cache → socket), sitting between `-z 0` and
`-z 1`. Its `-z` flag does not select a transmit path at all. If you want to
know which kernel mechanism is fastest on your hardware, this is the tier that
answers the question.

### A4. Default socket buffers may stall the pipe

**Problem.** TCP can keep at most `min(cwnd, rwnd)` bytes in flight, and the
receiver's advertised window is bounded by its receive buffer. Buffers below the
**bandwidth-delay product** (`BDP = bandwidth × RTT`) cap throughput below link
rate; buffers barely at BDP leave no headroom for RTT inflation under load.

**Action.** Data sockets request **128 MB** buffers on both sides
(`SO_SNDBUF` on the sender before `connect`, `SO_RCVBUF` on the receiver's
*listening* socket via `LISTEN_BUFF` and again on each accepted socket). The
control socket gets 4 MB. `print_sockbufs()` reads back and prints what the
kernel actually granted.

**Two details that are easy to get wrong and are right here:**

1. **Order matters.** The sender sets `SO_SNDBUF` *before* `connect()`, and the
   receiver sets `SO_RCVBUF` on the *listening* socket before any `accept()`.
   TCP's window-scale factor is negotiated in the three-way handshake from the
   buffer size in effect at that moment; setting it afterwards cannot raise the
   scale factor and silently caps the window. Setting it again on the accepted
   socket (as `receiver_thread` does) is harmless belt-and-braces.
2. **The request is advisory.** Linux caps `SO_SNDBUF`/`SO_RCVBUF` at
   `net.core.wmem_max` / `rmem_max` (commonly 208 KB by default!) and doubles
   what it does grant for bookkeeping. That is exactly why `print_sockbufs`
   exists — **if you did not raise `net.core.rmem_max`/`wmem_max` on both hosts,
   the printed value will be nowhere near 128 MB and your throughput will be
   capped no matter what this code asks for.**

**Result.** On this link RTT ≈ 100 µs, so BDP ≈ 10⁹ B/s · 10⁻⁴ s ≈ 125 KB.
128 MB is enormously more than that — three orders of magnitude of headroom. That
is generous to the point of being arbitrary (the adaptive tier's 16 MB is already
generous), but it costs only kernel memory and it decouples the application from
the network: a sender thread can dump a 32 MB chunk into the kernel and go pull
the next work item while TCP drains it. With a 128 MB send buffer, an entire
chunk fits in the socket, so `sendfile` on a warm page cache can return almost
immediately.

### A5. Thread migration cold-caches buffers and detaches threads from NIC queues

**Problem.** Linux's scheduler migrates threads between cores by default. Each
migration invalidates warm L1/L2 contents and can move a thread away from the
core handling its flow's NIC interrupt queue. It also adds run-to-run variance to
a benchmark.

**Action.** Both sides call `set_thread_affinity(thread_id, 4)`, pinning thread
*i* to core `i % 4` via `sched_setaffinity`.

**Result.** Stable cache residency, stable NUMA locality, reproducible numbers —
**for `-s 4` or fewer**. The core count is hardcoded to 4 rather than read from
`sysconf(_SC_NPROCESSORS_ONLN)`, so `-s 8` pins two threads to each of cores 0–3
and leaves the rest of the machine idle. See D1.

### A6. Reconnecting per repeat wastes round-trips — and this tier pays it deliberately

**Problem.** Opening fresh connections for every repeat costs, per repeat: a TCP
three-way handshake (1 RTT), a **slow-start ramp** (several RTTs before `cwnd`
reaches link capacity), TIME_WAIT accumulation on the closing side, and
`accept()` scheduling churn.

**Action.** The simple tier **does not** avoid this. `main`'s repeat loop calls
`connect_to_receiver` for all N streams every iteration, and each sender thread
`close()`s its socket at exit. The receiver spawns a fresh set of threads per
repeat, each of which `accept()`s anew on the (persistent) listening sockets.

**Why it is not simply an oversight.** The close *is* the end-of-stream marker
(Phase 3). With no terminator record in the protocol, there is no other way for
the receiver to know a stream is finished — the sender cannot say "that was the
last chunk" in-band. Persistence and terminator framing are a package deal;
adopting one requires the other. The adaptive tier took that package (its A6 +
`size == 0` terminators); this tier took neither.

**Result.** Each repeat pays roughly a handshake plus a slow-start ramp. On a
100 µs-RTT LAN, slow start reaches 10 Gb/s in on the order of a millisecond,
which against a ~0.4 s transfer is well under 1% — so on *this* link the cost is
real but small. It matters more than that in two cases: short transfers (where
the ramp is a large fraction of the total) and high repeat counts against a link
with any real RTT. It also means **repeat 1 and repeat 5 are measured under
identical conditions**, which is arguably a *feature* for a benchmark — the
adaptive tier's warm-`cwnd` persistence is why its FILE 2..N routinely clock
faster than FILE 1, an artifact this tier does not have.

### A7. The receiver's own copy can become the bottleneck

**Problem.** Even with a perfect zero-copy sender, the receiver must copy every
byte from the socket buffer into userspace. If you are trying to measure the
*network* path, that copy — plus, in save mode, the eventual `fwrite` of the
whole file — pollutes the measurement and can itself be the limiting factor.

**Action.** The third CLI argument, `save_to_disk`, selects between two receive
paths:

```c
if (data->rx_discard) {                                  // save_to_disk = 0
    ssize_t dropped = recv(data->client_fd, NULL, remaining, MSG_TRUNC);
} else {                                                 // save_to_disk = 1
    char *chunk_buffer = data->file_data + chunk_header.file_offset;
    ssize_t received = recv(data->client_fd, chunk_buffer + chunk_received, remaining, 0);
}
```

`MSG_TRUNC` on `recv` tells the kernel to consume and discard the bytes, and to
report how many it consumed, **without copying them to userspace** — so a `NULL`
buffer is legitimate. On Linux, TCP sockets support this; it is not portable
POSIX, and on other platforms `MSG_TRUNC` means something else entirely (on
datagram sockets it means "report the full datagram length even though you
truncated it"). Treat it as a Linux-specific optimization.

**Result.** `save_to_disk = 0` gives a genuine network-path measurement with the
receiver's memory bandwidth largely out of the picture; `save_to_disk = 1` gives
a real end-to-end file transfer with reassembly. Both are useful; using the wrong
one and comparing the numbers is a classic way to confuse yourself.

### A8. Wall clocks can jump mid-measurement

**Problem.** `CLOCK_REALTIME` can be stepped by NTP or the operator mid-run,
producing negative or nonsense durations.

**Action.** `get_time()` on both sides uses `CLOCK_MONOTONIC` (seconds since
boot, immune to clock steps) for every measurement, including the timestamps
embedded in the UDP progress datagrams.

**Result.** Durations are monotone and comparable across runs. Note the
measurement *window* differs per side: the sender times from thread start (after
`connect`) to thread exit, the receiver from after its `start_barrier` (so after
all N `accept`s) to thread exit. The receiver's number is therefore the cleaner
one — the sender's includes any straggling connect. Both exclude the file-header
exchange and the ready handshake, which is correct for a throughput figure.

---

## B. Correctness problems

### B1. `send`/`recv` legally do partial I/O

**Problem.** POSIX stream sockets guarantee neither that `send` accepts the whole
buffer (it takes what fits in the send buffer) nor that `recv` returns the amount
asked for — it returns whatever is currently in the kernel receive buffer. Code
that assumes full transfers corrupts framing the first time a buffer is full.
Additionally, signals interrupt syscalls with `EINTR`, and writing to a
peer-closed connection raises `SIGPIPE`, which kills the process by default.

**Action.** The **payload** paths all loop correctly — `sendfile`, `writev`, and
`sendmsg` each advance an offset or iovec and retry until `remaining == 0`,
treating `EINTR`/`EAGAIN` as retry. The receiver's chunk-header read loops
correctly too:

```c
while (header_received < (ssize_t)sizeof(chunk_header) && running) {
    ssize_t received = recv(fd, header_ptr + header_received,
                            sizeof(chunk_header) - header_received, 0);
    if (received < 0) { if (errno == EINTR || errno == EAGAIN) continue; ... }
    else if (received == 0) { goto thread_exit; }   // normal end of stream
    header_received += received;
}
```

**But the control-channel and chunk-header *sends* do not loop** — they are
single calls compared against the expected size (`send(control_sock, &header,
sizeof(header), 0) != sizeof(header)`, and the same pattern for the 24-byte chunk
header). See D3. There is also no `MSG_NOSIGNAL` and no `signal(SIGPIPE, SIG_IGN)`
anywhere in either program — a peer that dies mid-transfer kills the sender with
an uncatchable-by-default `SIGPIPE` rather than surfacing `EPIPE`. See D4.

**Result.** The bulk data path — 99.9999% of the bytes — is framed correctly and
survives short writes. The small control messages rely on the practical fact that
a 24-byte or 280-byte write into an empty multi-megabyte send buffer does not
fragment. That is true essentially always and wrong in principle.

### B2. Two hosts must agree on the byte-level layout of a struct

**Problem.** The two programs exchange `file_header_t` and `chunk_header_t` by
casting the struct to `char*` and sending its raw bytes. That makes the wire
format equal to the *compiler's in-memory layout*, which depends on three things:
integer endianness, the sizes of the types, and the padding the ABI inserts.

**Action.** Nothing — this is the tier's one significant protocol shortcut. There
are **no `htonl`/`htobe64` conversions anywhere in either file.** The wire format
is x86-64 System V, little-endian, and is defined by whatever the two compilers
happen to agree on.

Concretely, `chunk_header_t` is not 20 bytes but **24**:

```
offset  size  field
   0     4    uint32_t chunk_id
   4     4    <padding hole — inserted to 8-align the next field, NEVER INITIALIZED>
   8     8    uint64_t file_offset
  16     4    uint32_t chunk_size
  20     4    uint32_t padding   (explicitly set to 0)
```

and `file_header_t` is **280**, not 276 — 8 + 8 + 256 + 4 = 276, rounded up to
the struct's 8-byte alignment.

**Result.** It works, and on an x86-to-x86 lab link it will keep working. The
costs are: (a) the pair cannot interoperate with a big-endian or 32-bit peer, or
with a compiler using different padding rules; (b) four uninitialized stack bytes
are transmitted with every chunk header (D2); (c) the "padding" field is
misleadingly named — it documents the *trailing* pad while the *real* hazard is
the unnamed hole at offset 4. The adaptive tier's explicit field-by-field
`htobe64` encoding is strictly better practice, at the cost of more code. If you
ever want these two programs to talk to anything else, this is the first thing
to change.

### B3. A byte stream has no message boundaries

**Problem.** TCP is a *byte stream*, not a message stream. If sender and receiver
ever disagree about how many bytes belong to the previous message, the receiver's
next read of "a chunk header" actually consumes payload — and everything after it
is silently garbage. Worse, garbage parsed as a length field would drive a
multi-gigabyte `recv` or a buffer overflow.

**Action.** Three mechanisms, of differing strength:

1. **A magic number, once per run.** `file_header_t.magic` is checked against
   `0xdeadbeefcafebabe`; a mismatch aborts. This catches "you connected to the
   wrong program" but, being sent exactly once, cannot detect mid-run drift.
2. **Self-describing chunk headers.** Every payload is preceded by its
   `(chunk_id, file_offset, chunk_size)`, so the receiver never has to infer
   lengths. This is the load-bearing mechanism.
3. **A bounds check before every write:**

   ```c
   if (chunk_header.file_offset + chunk_header.chunk_size > data->total_file_size) {
       printf("Thread %d: Invalid chunk: ...\n", ...);
       break;
   }
   ```

   This is what stands between a desynchronized stream and a heap overflow of
   `file_data`, since `chunk_buffer = data->file_data + chunk_header.file_offset`
   is an unchecked pointer arithmetic otherwise. It is present and correct, and
   it is the single most important line in the receiver.

**Result.** Framing is sound for the intended trusted-peer case, and a desync
degrades to a clean abort rather than memory corruption. Two gaps worth naming:
there is no per-repeat sync marker, so a desync that happens to keep
`offset + size` in range would corrupt the reassembled file silently; and there
is no payload checksum, so bit errors that survive TCP's 16-bit checksum (not
unheard of at 10 Gb/s sustained rates) go undetected. This is a *trusted-peer,
trusted-link* protocol — fine for a point-to-point lab link, not for a hostile
or long-haul network (E).

### B4. Threads must not race the per-repeat state

**Problem.** On the receiver, N worker threads run concurrently and `main` must
not read their per-repeat metrics until every one of them has finished writing.
And every thread must start its clock at the same moment, or the throughput
figures measure connection setup skew rather than the network.

**Action.** Two `pthread_barrier_t`s with **deliberately different counts**:

```
 receiver_thread i                         main
 ─────────────────                         ────
 accept() on listen_fds[i]
 set TCP_NODELAY, SO_RCVBUF
 ── START BARRIER (count = N) ──           (not a participant)
 start_time = get_time()
 loop: recv chunk hdr + payload
       write to file_data + offset
       UDP progress every 16 MB / 0.5 s
 (EOF) close socket, record metrics
 ── END BARRIER (count = N + 1) ── ◄────── ── END BARRIER (count = N + 1) ──
 return                                    pthread_join × N
                                           sum metrics; send 'R'
```

- **Start barrier, count N** — `main` is *not* a member. All N threads must have
  accepted their connection before any of them starts its clock, so all N measure
  the same interval, and thread 0's `TRANSFER_START` UDP event fires when the
  transfer genuinely begins.
- **End barrier, count N + 1** — `main` *is* the extra member. It blocks there
  instead of going straight to `pthread_join`, which means it is released the
  instant the last thread finishes its data loop rather than in thread-index
  order. The subsequent `pthread_join` calls then return immediately and serve
  only to reap the threads.

The sender needs no barriers at all: its work queue is lock-free, its threads
share nothing mutable, and `pthread_join` alone provides the
happens-before edge that makes `thread_data[i]` safe for `main` to read.

**Result.** Clean per-repeat supersteps with correctly synchronized clocks. The
`+1` end barrier is the subtle bit and it is right. The failure mode is what
happens when a thread *doesn't* arrive — see D5, which is a real deadlock.

### B5. Cross-thread flags without atomics

**Problem.** `running`, `coordinator.should_exit`, and `udp_initialized` are
polled across threads and mutated from a signal handler. A compiler may legally
cache a plain `int` in a register indefinitely, so a polling loop would never
observe the update.

**Action.** They are declared `volatile int`, which forbids register caching.

**Result.** The polling loops work — but `volatile` provides **no atomicity and
no memory ordering** in the C11 model; these should be `_Atomic` or, for the
signal-handler-written ones, `volatile sig_atomic_t`. It holds in practice on x86
(strong memory model, aligned word stores are atomic). Note the inconsistency:
this same file uses `_Atomic int` correctly for `current_item` in the work queue,
so the author clearly knows the tool — the flags simply predate or escaped that
choice. A common but formally unsound idiom worth recognizing (E).

Separately, the signal handler calls `printf`, which is **not async-signal-safe**
(it takes a lock on `stdout`; a `SIGINT` arriving while the main thread holds
that lock deadlocks the process). `write(2)` to fd 1 is the safe equivalent.

### B6. The ready signal must be unambiguous

**Problem.** A one-byte signal shares a connection with nothing else here, but it
still has to be distinguishable from a closed connection and from garbage.

**Action.** `send_ready_signal` sends the single ASCII byte `'R'` (0x52);
`wait_for_ready_signal` requires both `recv` to return exactly 1 *and* the byte
to equal `'R'`, returning `-1` otherwise.

**Result.** Adequate and appropriately minimal. The value check means a stray
byte does not silently advance the state machine. Two things it lacks relative to
the adaptive tier's control channel: there is **no ACK** (the sender does not
confirm receipt, so the receiver never learns whether the sender is alive) and
**no timeout** — `wait_for_ready_signal` is a bare blocking `recv`, so a receiver
that dies without closing its socket hangs the sender until TCP's retransmission
timeout gives up, which can take ~15 minutes. See D6.

---

## C. Coordination problems

### C1. When may the sender start the next repeat?

**Problem.** If the sender opens repeat *k+1*'s connections while the receiver is
still finishing repeat *k*, the receiver's threads for repeat *k* have already
closed their sockets and the new connections would sit in the listen backlog —
usually harmless, but the *metrics* would then be recorded against overlapping
transfers, and with `save_to_disk` the reassembly buffer would be written
concurrently by two repeats.

**Action.** An explicit application-level handshake on the control connection.
The receiver sends `'R'` (a) once before the first repeat and (b) after each
repeat *except the last*. The sender blocks in `wait_for_ready_signal` at the
same points. Crucially the receiver sends it only **after** `main` has passed the
end barrier and joined all N threads — so `'R'` means "all my threads for the
previous repeat are fully done and reaped."

**Result.** The sender never opens repeat *k+1*'s sockets before the remote
*application* has confirmed it finished repeat *k* — the end-to-end argument in
miniature: reliability that matters to the application must be confirmed at the
application layer, not inferred from `send()` returning success (which only means
the byte entered the *local* kernel buffer).

### C2. Data connections must be established without a lost-race

**Problem.** The sender `connect`s to `base_port + 1 + i` immediately after
receiving `'R'`. But the receiver sends `'R'` *before* spawning the next repeat's
threads — so at the moment the sender connects, there may be **no thread in
`accept()` at all**. In a naive design that connection is refused.

**Action.** Two things make it safe. First, the *listening* sockets are created
**once**, before the repeat loop, and outlive every repeat — only the *accepted*
sockets are per-repeat. Second, `create_listener` passes a backlog of **128**:

```c
if (listen(listen_fd, 128) < 0) { ... }
```

A `connect` to a listening socket with backlog space completes the three-way
handshake **in the kernel**, with no application involvement; the connection sits
in the accept queue until someone calls `accept()`. The sender's connect succeeds
immediately and it can even start pushing data into the socket before the
receiver thread exists.

**Result.** The receiver's "send `'R'`, *then* spawn threads" ordering is safe,
and the sender never blocks waiting for the receiver to schedule a thread. A
backlog of 128 against 4 pending connections is enormous headroom. (This is also
why moving the listener creation inside the repeat loop — a tempting
simplification — would break the run.)

### C3. The two processes must start in the right order

**Problem.** Connections need a listener before a connector. Unlike the adaptive
tier, connections here go in **one direction only** (sender → receiver, for both
control and data), which makes this simpler than it could be.

**Action.** The startup sequences interlock:

**Receiver `main`:** (1) set up the UDP progress socket; (2) `create_listener(base_port)`
and block in `accept()`; (3) receive and validate the header; (4) allocate the
reassembly buffer and both barriers; (5) create listeners for `base_port+1 .. +N`;
(6) send the first `'R'`; (7) enter the repeat loop.

**Sender `main`:** (1) load or `stat` the file per `-z`; (2) connect to
`base_port`; (3) send the header; (4) block for `'R'`; (5) enter the repeat loop,
connecting N data sockets per iteration.

The ordering is safe because the receiver creates its **data** listeners
(step 5) before sending the first `'R'` (step 6), and the sender does not connect
to any data port until it has received that `'R'`. The handshake that exists for
repeat pacing (C1) doubles as the startup interlock.

**Result.** "Start the receiver first" is the only requirement, and it applies to
exactly one connection — the control socket. Everything downstream is gated by
`'R'`. Note there is **no retry** on the sender's initial `connect`: start the
sender first and it exits immediately with `connect: Connection refused` rather
than waiting. The adaptive tier's receiver retries its control connect 10 times
with exponential backoff; this tier does not.

### C4. The web UI needs live progress without touching the data path

**Problem.** The Flask UI wants a progress bar, but anything that makes the
receiver's data threads block on a UI — a TCP connection to the web server, a
write to a log file, a mutex around shared progress state — puts UI latency
directly into the transfer's critical path.

**Action.** Progress is pushed as **fire-and-forget UDP datagrams to loopback**:

```c
snprintf(msg, sizeof(msg), "PROGRESS:%d:%d:%zu:%zu:%d:%.3f",
         transfer_num, thread_id, bytes_received, total_bytes,
         chunks_received, get_time());
sendto(udp_sock, msg, strlen(msg), 0, (struct sockaddr*)&udp_addr, sizeof(udp_addr));
```

with `TRANSFER_START` / `TRANSFER_COMPLETE` events emitted by thread 0 only. Rate
limiting is **whichever comes first, 16 MB or 0.5 s**, which bounds the datagram
rate under both fast and stalled conditions. The socket is set up once; if setup
fails, `udp_initialized` stays 0 and every call becomes a no-op — the transfer
proceeds normally with no UI.

**Result.** `sendto` on a connectionless loopback socket returns as soon as the
datagram is queued; there is no handshake, no retransmission, no blocking, and no
dependency on the UI process even existing. If the UI is down or slow, datagrams
are simply dropped — the correct trade for telemetry, where a lost sample is
worth far less than a stalled transfer. The `%.3f` monotonic timestamp lets the
UI compute rates itself and detect gaps. Note the destination is hardcoded to
`127.0.0.1:5666`, so the UI must run on the HPC host.

---

## D. Defects found in review

Unlike the adaptive tier's D section, these are **present in the current
snapshot**, not already fixed. They are ordered roughly by severity.

### D1. Hardcoded core count breaks `-s > 4`

**Problem.** Both `sender_thread` and `receiver_thread` call
`set_thread_affinity(data->thread_id, 4)`. The `4` is a literal, unrelated to
`num_streams` and unrelated to the machine. With `-s 8`, threads 4–7 are pinned
to cores 0–3 alongside threads 0–3, doubling up on four cores while every other
core on the box sits idle. Throughput will look inexplicably flat or worse as you
raise `-s` past 4 — and the natural conclusion ("more streams don't help") would
be an artifact of this line, not a property of the network.

**Fix.** Read the actual core count and pass it through:

```c
int ncores = (int)sysconf(_SC_NPROCESSORS_ONLN);
set_thread_affinity(data->thread_id, ncores > 0 ? ncores : 1);
```

Better still, store it in `thread_data_t` so both sides use one value, and
consider skipping affinity entirely when `num_streams > ncores`.

### D2. Uninitialized padding bytes are transmitted with every chunk header

**Problem.** `chunk_header_t` has a 4-byte hole at offset 4 (B2). The designated
initializer sets `.chunk_id`, `.file_offset`, `.chunk_size`, `.padding` — every
*named* member — but C leaves the value of unnamed padding **unspecified**. Those
four bytes come from whatever was on the stack, and they go out on the wire with
every single chunk header. `file_header_t` has the same issue in its 4 trailing
pad bytes, and additionally `filename[256]` is only partially written by
`strncpy`, so the unused tail is uninitialized heap/stack too — up to ~250 bytes
of process memory transmitted at startup.

This is harmless on a trusted link (the receiver never reads those bytes) but it
is a textbook **information disclosure**, it makes captures non-reproducible, and
it will make any future checksum over the header non-deterministic.

**Fix.** Zero the structs before filling them:

```c
chunk_header_t chunk_header;
memset(&chunk_header, 0, sizeof(chunk_header));
chunk_header.chunk_id    = work_item.chunk_id;
chunk_header.file_offset = work_item.file_offset;
chunk_header.chunk_size  = work_item.chunk_size;
```

and the same for `file_header_t` before the `strncpy`. (Fixing B2 properly — an
explicit field-by-field big-endian serializer into a fixed-size byte buffer —
subsumes this defect entirely and is the better long-term answer.)

### D3. Small sends are not looped

**Problem.** Three sends are issued as single calls and their result compared for
equality with the full length: the 280-byte `file_header_t`, the 24-byte chunk
header, and the 1-byte `'R'`. A partial `send` is legal whenever the socket send
buffer is nearly full, and it would be reported here as a hard error
(`perror("send header")`, thread aborts) even though nothing is actually wrong.
For the chunk header the consequence is worse than a spurious error: if a partial
send were *not* detected, the receiver's framing would desync permanently.

**Fix.** A `send_all` helper of the kind the adaptive tier uses:

```c
static ssize_t send_all(int fd, const void *buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, (const char*)buf + total, len - total, MSG_NOSIGNAL);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) return -1;
        total += n;
    }
    return (ssize_t)total;
}
```

and a matching `recv_all` for the receiver's `file_header_t` read, which has the
identical problem in the other direction.

### D4. `SIGPIPE` kills the sender instead of surfacing `EPIPE`

**Problem.** No `send` passes `MSG_NOSIGNAL`, and neither program installs
`signal(SIGPIPE, SIG_IGN)`. If the receiver dies or closes a data socket early,
the sender's next write raises `SIGPIPE`, whose default disposition terminates
the process — so all the careful `perror`/`goto thread_exit` error handling in
`sender_thread` is unreachable in exactly the scenario it was written for. The
RDB Flask service would see the sender vanish with no `=== FINAL AVERAGE
RESULTS ===` block and no diagnostic.

**Fix.** `signal(SIGPIPE, SIG_IGN)` in both `main`s, and `MSG_NOSIGNAL` on every
`send`/`sendmsg` (as the `send_all` above does). `sendfile` and `writev` are not
covered by `MSG_NOSIGNAL`, so the `SIG_IGN` is required regardless.

### D5. A failed `accept()` deadlocks the receiver permanently

**Problem.** `receiver_thread` begins:

```c
data->client_fd = accept(data->listen_fd, ...);
if (data->client_fd < 0) { perror("accept"); return NULL; }
```

That early return skips **both** barriers. `start_barrier` (count N) then never
releases, so the other N−1 threads block forever at it; `end_barrier` (count
N+1) never releases, so `main` blocks forever too. The process hangs with no
output and must be killed. `accept` can fail for entirely routine reasons —
`EMFILE`, `ECONNABORTED`, or `EINTR` from the `SIGINT` handler, which is the
likeliest one in practice: **pressing Ctrl-C while the receiver waits for the
next repeat's connections hangs it instead of shutting it down.**

**Fix.** Retry on `EINTR`/`ECONNABORTED`, and on genuine failure set
`should_exit` and still arrive at both barriers before returning — the same
"show up one last time so peers can pass" discipline the adaptive tier uses on
every error path:

```c
if (data->client_fd < 0) {
    perror("accept");
    global_coordinator->should_exit = 1;
    pthread_barrier_wait(&global_coordinator->start_barrier);
    pthread_barrier_wait(&global_coordinator->end_barrier);
    return NULL;
}
```

The same audit applies to every `goto thread_exit` in the data loop — those *do*
reach `end_barrier`, correctly, but only because the label sits above it.

### D6. `wait_for_ready_signal` can block for ~15 minutes

**Problem.** It is a bare blocking `recv` with no timeout. If the receiver
process is killed with `SIGKILL` or its host loses power, the sender sits in that
`recv` until TCP's retransmission timeout expires — on Linux, backing off toward
roughly 15 minutes. From the Flask UI's perspective the transfer has simply hung.

**Fix.** `select()`/`poll()` with a bounded timeout around the `recv`, plus
`SO_KEEPALIVE` with aggressive parameters on the control socket. The adaptive
tier's `comprehensive_socket_health_check` and 8 s ACK timeout are the reference
implementation of this pattern in the same codebase.

### D7. `signal(SIGKILL, ...)` is a no-op

**Problem.** The receiver registers `signal(SIGKILL, signal_handler)`. `SIGKILL`
cannot be caught, blocked, or ignored — the call fails and returns `SIG_ERR`,
which is discarded. The receiver therefore has **no `SIGTERM` handler at all**,
so an ordinary `kill` or a container stop terminates it without running the
cleanup path, without saving the file, and without printing results. The sender
registers `SIGTERM` correctly, so this is an inconsistency between the two files.

**Fix.** `signal(SIGTERM, signal_handler);` — almost certainly what was intended.

### D8. Throughput arithmetic is optimistic and the Gbps conversion is wrong

**Problem.** Two independent issues in the same three lines, on both sides:

1. **Summed rates over differing intervals.** `total_throughput` sums each
   thread's `bytes_i / duration_i`. That equals the true aggregate only if every
   thread ran over the same interval. A thread that finishes early has a higher
   individual rate and inflates the sum, while `max_duration` — the reported
   duration — is the *slowest* thread's. The reported throughput is therefore
   systematically ≥ the honest figure `Σbytes / (max_end − min_start)`.
2. **A units bug.** `throughput` is computed in **MiB/s** (dividing by
   `1024.0 * 1024.0`), but the conversion is
   `avg_throughput_gbps = avg_throughput * 8 / 1000.0`, which is the formula for
   **MB/s → Gbps**. 1 MiB/s is 8.388608 Mbit/s, not 8. **Every Gbps figure this
   program prints — and every figure the RDB Flask service parses out of the
   `=== FINAL AVERAGE RESULTS ===` block — is 4.6% too low.**

**Fix.** For (2), `gbps = mib_per_s * 8.388608 / 1000.0`, or better, compute
bytes/s in decimal units throughout and stop mixing MiB with Gb. For (1), track
`min(start_time)` and `max(end_time)` across threads and divide total bytes by
that union interval, exactly as the adaptive tier does.

**Third, smaller issue:** if the run is cut short by Ctrl-C, both `main`s still
divide their accumulated sums by the full `repeat_count`, understating the
averages. Count completed repeats and divide by that.

### D9. Error paths in the repeat loop leak and skip cleanup

**Problem.** Inside the sender's repeat loop:

```c
int sock = connect_to_receiver(receiver_ip, port, 128 * 1024 * 1024);
if (sock < 0) { printf("Failed to connect stream %d to port %d\n", i, port); return 1; }
```

This `return 1` — and the `pthread_create` failure path just below it — leaves
`control_sock` and `file_fd` open, `file_data` and the work queue allocated, and
any threads already created for this repeat **running and detached from any
join**, writing into a `thread_data` array on a stack frame that is about to
disappear. In practice the process exits immediately so nothing observable
happens, but it is undefined behavior on paper and a real hazard if this code is
ever refactored into a library or long-running service (which the Flask
integration nudges it toward).

**Fix.** A single `goto cleanup:` label at the bottom of `main` — the receiver
already uses exactly this pattern and it should be mirrored on the sender.

### D10. Untrusted header fields drive `malloc` and a filesystem path

**Problem.** The receiver does `malloc(header.file_size)` with no upper bound
(the adaptive tier caps at 10 GB), and builds its output path with
`snprintf(path, sizeof(path), "%s/%s", output_dir, header.filename)` where
`filename` came off the wire. It is also not guaranteed NUL-terminated by the
protocol — the sender happens to terminate it, but a garbled or hostile peer need
not, and `printf("%s", header.filename)` would then read past the buffer. A
`filename` of `../../etc/something` would write outside `output_dir`.

**Fix.** Force `header.filename[255] = '\0'` immediately after receipt; reject
any filename containing `/` or starting with `.`; sanity-cap `file_size`. Cheap
insurance even on a trusted link, since the same bytes could arrive corrupted
rather than malicious.

---

## E. Open problems — not yet solved

- **Single file per run.** The protocol carries one `file_header_t` for the whole
  session, so transferring a second file means restarting both processes. The
  adaptive tier's per-file headers and READY/ACK-per-file loop exist precisely to
  lift this restriction.
- **Reassembly requires the whole file in RAM.** `save_to_disk = 1` `malloc`s
  `file_size` bytes up front and does one `fwrite` at the very end. A 100 GB
  transfer needs 100 GB of RAM, and nothing is on disk until the last repeat
  completes. Writing chunks to a pre-`ftruncate`d file via `pwrite(fd, buf, size,
  offset)` — which is thread-safe and needs no shared file position — would make
  memory use constant and is a natural next step, since every chunk already
  carries its destination offset.
- **No adaptivity.** `CHUNK_SIZE` is a compile-time constant and there is no
  per-thread performance tracking at all. This is by design — it is what makes
  this the baseline the adaptive tier is measured against — but it means a
  persistently slow stream is never compensated for beyond what the 32 MB grain
  allows.
- **Grain is coarse relative to file size.** 16 work items for 4 threads on a
  500 MB file leaves little room for the work queue to balance anything, and
  files under 128 MB will leave threads with no work (A2).
- **No payload checksum or authentication** — trusted point-to-point link
  assumed (B3).
- **Wire format is the compiler's struct layout**, not a defined protocol (B2).
- **`volatile` instead of `_Atomic`** for cross-thread flags, and a non-async-signal-safe
  `printf` in the signal handler (B5).
- **The UDP progress destination is hardcoded** to `127.0.0.1:5666`, so the web
  UI must be co-located with the receiver.
- **The sender does not retry its initial connect**, so process start order is a
  hard requirement rather than a preference (C3).

---

## Build and run

```bash
# Sender (RDB)
gcc -O3 -Wall -pthread -std=gnu11 -o simple_sender_rdb simple_sender_rdb.c -lm

# Receiver (HPC)
gcc -O3 -Wall -pthread -std=gnu11 -o simple_receiver_hpc simple_receiver_hpc.c -lm
```

```bash
# 1. Start the receiver first (C3):
./simple_receiver_hpc 5303 4 1 ./received
#                     port streams save_to_disk output_dir
#   save_to_disk: 1 = reassemble in RAM and write the file at the end
#                 0 = discard payload with MSG_TRUNC (pure network benchmark, A7)

# 2. Then the sender:
./simple_sender_rdb 169.254.207.40 /path/to/file.bin -s 4 -z 1 -p 5303 -r 5
#                   receiver_ip    file
#   -s  streams   (default 4; do not exceed 4 until D1 is fixed)
#   -z  0=writev  1=sendfile (default)  2=MSG_ZEROCOPY        (A3)
#   -p  base port (default 5303; control = 5303, data = 5304..5303+N)
#   -r  repeats   (default 1)
```

**Before trusting any number**, raise the kernel buffer caps on *both* hosts or
the 128 MB `SO_RCVBUF`/`SO_SNDBUF` requests will be silently clamped (A4) — the
`print_sockbufs` output at startup tells you what you actually got:

```bash
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
```

To compare transmit paths, hold everything else constant and sweep `-z`:

```bash
for z in 0 1 2; do ./simple_sender_rdb 169.254.207.40 test.bin -s 4 -z $z -r 5; done
```

Use `save_to_disk = 0` on the receiver for that comparison so its memory
bandwidth does not mask the difference — and remember every reported Gbps figure
is currently 4.6% low (D8).

---

## Quick reference: problem → solution, one line each

| Problem | Solution | Section |
|---|---|---|
| One flow / one core can't fill 10 Gb/s | N parallel TCP streams across N cores and NIC queues | A1 |
| Stragglers idle the other threads | Shared work queue, lock-free `atomic_fetch_add` claim | A2 |
| Would a queue lock bottleneck? | No lock at all; ~37 claims/s total, cache line cold between | A2 |
| Userspace copies cap throughput | Three selectable TX paths: writev / sendfile / MSG_ZEROCOPY | A3 |
| Threads sharing one file descriptor | `sendfile` takes offset by pointer — no shared file position | A3 |
| Buffers below BDP cap throughput | 128 MB data buffers, set *before* connect/accept for window scaling | A4 |
| Kernel silently clamps buffer requests | `print_sockbufs` reads back and prints the granted size | A4 |
| Thread migration cold-caches buffers | CPU affinity: thread *i* → core `i % 4` (hardcoded — D1) | A5 |
| Per-repeat reconnect costs handshake + slow start | Accepted as the price of close-as-terminator framing | A6 |
| Receiver's own copy pollutes the measurement | `save_to_disk = 0` → `recv(..., NULL, n, MSG_TRUNC)` discard path | A7 |
| Wall clock jumps mid-benchmark | `CLOCK_MONOTONIC` everywhere, including UDP timestamps | A8 |
| Partial `send`/`recv` corrupt framing | Payload paths loop correctly; small sends do not (D3) | B1 |
| Endianness / struct layout on the wire | Not handled — raw native structs, x86-to-x86 only | B2 |
| Byte-stream desync corrupts silently | Magic (once), self-describing chunk headers, `offset+size` bounds check | B3 |
| Threads race per-repeat state | Start barrier (N) for synced clocks; end barrier (N+1) includes `main` | B4 |
| Compiler may cache cross-thread flags | `volatile` (works on x86; `_Atomic` is the correct fix) | B5 |
| Ready signal must be unambiguous | Single `'R'` byte, value-checked; no ACK, no timeout | B6 |
| When may the next repeat start? | `'R'` sent only after all threads joined — app-level confirmation | C1 |
| Sender connects before receiver `accept`s | Persistent listeners + backlog 128 absorb it in-kernel | C2 |
| Startup ordering races | Receiver-first; data listeners exist before the first `'R'` | C3 |
| UI progress must not block the data path | Fire-and-forget UDP to loopback, rate-limited 16 MB / 0.5 s | C4 |
| `-s > 4` silently double-pins cores | Use `sysconf(_SC_NPROCESSORS_ONLN)` (**open**) | D1 |
| Uninitialized padding sent every chunk | `memset` the structs, or serialize field-by-field (**open**) | D2 |
| Small sends unlooped | Add `send_all`/`recv_all` (**open**) | D3 |
| `SIGPIPE` kills the sender | `signal(SIGPIPE, SIG_IGN)` + `MSG_NOSIGNAL` (**open**) | D4 |
| Failed `accept()` deadlocks the receiver | Arrive at both barriers on every error path (**open**) | D5 |
| Dead peer hangs the sender ~15 min | `select()` timeout + keepalive on the control socket (**open**) | D6 |
| `signal(SIGKILL, ...)` is a no-op | Should be `SIGTERM` (**open**) | D7 |
| Reported Gbps is 4.6% low | MiB/s × 8.388608, not × 8; and use the union interval (**open**) | D8 |
| Error paths in the repeat loop leak | Single `goto cleanup` as the receiver already does (**open**) | D9 |
| Untrusted header drives `malloc` and a path | Cap `file_size`, NUL-terminate and sanitize `filename` (**open**) | D10 |
