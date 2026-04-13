// Compile and Run
// gcc -O3 -Wall -pthread -std=gnu11 -o simple_sender_rdb simple_sender_rdb.c -lm
// ./simple_sender_rdb 169.254.207.40 my.bin -s 4 -z 1 -p 5303 -r 5
//   zerocopy modes: 0=writev (fallback), 1=sendfile, 2=MSG_ZEROCOPY

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sched.h>
#include <math.h>
#include <stdatomic.h>
#include <sys/sendfile.h>
#include <getopt.h>

#define DEFAULT_STREAMS 4
#define DEFAULT_PORT 5303
#define CHUNK_SIZE (32 * 1024 * 1024)  // 32MB chunks
#define MAGIC_HEADER 0xdeadbeefcafebabe

#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY 60
#endif

typedef struct {
    uint32_t chunk_id;
    uint64_t file_offset;
    uint32_t chunk_size;
    uint32_t padding;
} chunk_header_t;

typedef struct {
    uint64_t magic;
    uint64_t file_size;
    char filename[256];
    int repeat_count;  // Add repeat count to header
} file_header_t;

typedef struct {
    uint32_t chunk_id;
    uint64_t file_offset;
    uint32_t chunk_size;
} work_item_t;

typedef struct {
    work_item_t *items;
    int total_items;
    _Atomic int current_item; // lock-free
} work_queue_t;

typedef struct {
    int thread_id;
    int socket_fd;
    int file_fd;
    const char *filename;
    char *file_data;
    size_t total_size;
    int use_zerocopy; // 0=writev, 1=sendfile, 2=MSG_ZEROCOPY
    work_queue_t *work_queue;
    double throughput_mbps;
    double duration;
    int chunks_processed;
} thread_data_t;

static volatile int running = 1;

static inline void print_sockbufs(int fd, const char *tag, int is_tx) {
    int sz = 0; socklen_t sl = sizeof(sz);
    int lev = SOL_SOCKET, opt = is_tx ? SO_SNDBUF : SO_RCVBUF;
    if (getsockopt(fd, lev, opt, &sz, &sl) == 0) {
        printf("%s actual %s buffer: %d bytes\n", tag, is_tx ? "send" : "recv", sz);
    }
    int nodelay = 0; sl = sizeof(nodelay);
    if (getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, &sl) == 0) {
        printf("%s TCP_NODELAY: %s\n", tag, nodelay ? "on" : "off");
    }
}

void signal_handler(int sig) { running = 0; printf("\nShutdown requested...\n"); }

double get_time() { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec + ts.tv_nsec / 1e9; }

int load_file(const char *filename, char **data, size_t *size) {
    FILE *file = fopen(filename, "rb");
    if (!file) { perror("fopen"); return -1; }
    fseek(file, 0, SEEK_END); long sz = ftell(file);
    if (sz < 0) { perror("ftell"); fclose(file); return -1; }
    *size = (size_t)sz;
    fseek(file, 0, SEEK_SET);
    *data = malloc(*size);
    if (!*data) { perror("malloc"); fclose(file); return -1; }
    if (fread(*data, 1, *size, file) != *size) { perror("fread"); free(*data); fclose(file); return -1; }
    fclose(file); return 0;
}

int connect_to_receiver(const char *ip, int port, int want_txbuf_bytes) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return -1; }

    int flag = 1; setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    if (want_txbuf_bytes > 0) setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &want_txbuf_bytes, sizeof(want_txbuf_bytes));

    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(port); inet_pton(AF_INET, ip, &addr.sin_addr);
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("connect"); close(sock); return -1; }

    print_sockbufs(sock, "sock", 1);
    return sock;
}

int create_work_queue(work_queue_t *queue, size_t file_size) {
    queue->total_items = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    queue->items = malloc(queue->total_items * sizeof(work_item_t));
    if (!queue->items) { perror("malloc work_queue items"); return -1; }
    atomic_store(&queue->current_item, 0);

    for (int i = 0; i < queue->total_items; i++) {
        queue->items[i].chunk_id = i;
        queue->items[i].file_offset = (uint64_t)i * CHUNK_SIZE;
        queue->items[i].chunk_size = (i == queue->total_items - 1)
            ? (uint32_t)(file_size - queue->items[i].file_offset)
            : (uint32_t)CHUNK_SIZE;
    }

    return 0;
}

int get_next_work_item(work_queue_t *queue, work_item_t *item) {
    int idx = atomic_fetch_add(&queue->current_item, 1);
    if (idx >= queue->total_items) return -1;
    *item = queue->items[idx];
    return 0;
}

void cleanup_work_queue(work_queue_t *queue) { free(queue->items); }

int set_thread_affinity(int thread_id, int num_cores) {
    cpu_set_t cpuset; CPU_ZERO(&cpuset);
    CPU_SET(thread_id % num_cores, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0) {
        return 0;
    } else { perror("sched_setaffinity"); return -1; }
}

void prefetch_chunk_data(char *data, size_t size) {
    const size_t cache_line = 64;
    for (size_t i = 0; i < size; i += cache_line * 8) __builtin_prefetch(data + i, 0, 3);
}

// Drain error queue for MSG_ZEROCOPY completions (best-effort, non-blocking)
static void drain_zerocopy_completions(int sock) {
    char ctrl[1024]; char buf[1];
    struct iovec iov = { .iov_base = buf, .iov_len = sizeof(buf) };
    struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1, .msg_control = ctrl, .msg_controllen = sizeof(ctrl) };
    for (;;) {
        ssize_t r = recvmsg(sock, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            else break; // ignore other errors here
        }
    }
}

void* sender_thread(void* arg) {
    thread_data_t *data = (thread_data_t*)arg;
    set_thread_affinity(data->thread_id, 4);

    // Enable MSG_ZEROCOPY if requested (mode 2)
    if (data->use_zerocopy == 2) {
        int one = 1;
        if (setsockopt(data->socket_fd, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one)) != 0) {
            data->use_zerocopy = 0;
        }
    }

    double start_time = get_time();
    size_t total_sent = 0;
    int chunks_processed = 0;

    work_item_t work_item;
    while (get_next_work_item(data->work_queue, &work_item) == 0 && running) {
        // Send header first (small copy)
        chunk_header_t chunk_header = { .chunk_id = work_item.chunk_id,
                                        .file_offset = work_item.file_offset,
                                        .chunk_size = work_item.chunk_size,
                                        .padding = 0 };
        ssize_t hs = send(data->socket_fd, &chunk_header, sizeof(chunk_header), 0);
        if (hs != (ssize_t)sizeof(chunk_header)) { perror("send header"); goto thread_exit; }
        total_sent += hs;

        // Then send payload with selected path
        if (data->use_zerocopy == 1) {
            // sendfile: kernel pulls from file pages
            off_t off = (off_t)work_item.file_offset;
            size_t remaining = work_item.chunk_size;
            while (remaining > 0 && running) {
                ssize_t s = sendfile(data->socket_fd, data->file_fd, &off, remaining);
                if (s < 0) { if (errno == EINTR || errno == EAGAIN) continue; perror("sendfile"); goto thread_exit; }
                remaining -= s; total_sent += s;
            }
        } else if (data->use_zerocopy == 2) {
            // MSG_ZEROCOPY: sendmsg with flag (data from user buffer)
            char *chunk_data = data->file_data + work_item.file_offset;
            prefetch_chunk_data(chunk_data, work_item.chunk_size);
            struct iovec iov = { .iov_base = chunk_data, .iov_len = work_item.chunk_size };
            struct msghdr msg = { 0 };
            msg.msg_iov = &iov; msg.msg_iovlen = 1;
            size_t remaining = work_item.chunk_size;
            while (remaining > 0 && running) {
                ssize_t s = sendmsg(data->socket_fd, &msg, MSG_ZEROCOPY);
                if (s < 0) { if (errno == EINTR || errno == EAGAIN) { drain_zerocopy_completions(data->socket_fd); continue; } perror("sendmsg MSG_ZEROCOPY"); goto thread_exit; }
                remaining -= s; total_sent += s;
                iov.iov_base = (char*)iov.iov_base + s; iov.iov_len -= s;
                drain_zerocopy_completions(data->socket_fd); // opportunistic
            }
        } else {
            // Fallback: writev
            char *chunk_data = data->file_data + work_item.file_offset;
            prefetch_chunk_data(chunk_data, work_item.chunk_size);
            struct iovec iov[1] = { { .iov_base = chunk_data, .iov_len = work_item.chunk_size } };
            size_t remaining = work_item.chunk_size;
            while (remaining > 0 && running) {
                ssize_t s = writev(data->socket_fd, iov, 1);
                if (s < 0) { if (errno == EINTR || errno == EAGAIN) continue; perror("writev"); goto thread_exit; }
                remaining -= s; total_sent += s;
                iov[0].iov_base = (char*)iov[0].iov_base + s; iov[0].iov_len -= s;
            }
        }

        chunks_processed++;
    }

thread_exit: {
        if (data->use_zerocopy == 2) drain_zerocopy_completions(data->socket_fd);

        double end_time = get_time();
        double duration = end_time - start_time;
        double throughput = duration > 0 ? (total_sent / (1024.0 * 1024.0)) / duration : 0;
        data->throughput_mbps = throughput;
        data->duration = duration;
        data->chunks_processed = chunks_processed;

        close(data->socket_fd);
        return NULL;
    }
}

int wait_for_ready_signal(int control_sock) {
    char ready_signal;
    ssize_t received = recv(control_sock, &ready_signal, 1, 0);
    if (received != 1 || ready_signal != 'R') {
        return -1;
    }
    return 0;
}

void print_usage(const char *program_name) {
    printf("Simple High-Performance File Sender (Repeat Transfer)\n");
    printf("Usage: %s <receiver_ip> <file_path> [options]\n", program_name);
    printf("Options:\n");
    printf("  -s <streams>    Number of streams (default: 4)\n");
    printf("  -z <zerocopy>   Zero-copy mode: 0=writev, 1=sendfile, 2=MSG_ZEROCOPY (default: 1)\n");
    printf("  -p <port>       Base port (default: 5303)\n");
    printf("  -r <repeats>    Number of repeat transfers (default: 1)\n");
    printf("  -h              Show this help\n");
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    const char *receiver_ip = argv[1];
    const char *filename = argv[2];
    int num_streams = DEFAULT_STREAMS;
    int use_zerocopy = 1; // default to sendfile
    int base_port = DEFAULT_PORT;
    int repeat_count = 1;

    // Parse command line options
    int opt;
    while ((opt = getopt(argc, argv, "s:z:p:r:h")) != -1) {
        switch (opt) {
            case 's':
                num_streams = atoi(optarg);
                break;
            case 'z':
                use_zerocopy = atoi(optarg);
                break;
            case 'p':
                base_port = atoi(optarg);
                break;
            case 'r':
                repeat_count = atoi(optarg);
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (repeat_count < 1) repeat_count = 1;

    printf("High-Performance File Sender (Optimized with Repeats)\n");
    printf("=====================================================\n");
    printf("Target: %s:%d\n", receiver_ip, base_port);
    printf("File: %s\n", filename);
    printf("Streams: %d\n", num_streams);
    printf("Zero-copy mode: %d (0=writev,1=sendfile,2=MSG_ZEROCOPY)\n", use_zerocopy);
    printf("Repeat count: %d\n", repeat_count);
    printf("Optimizations: Zero-copy TX, CPU Affinity, Lock-free work queue\n\n");

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Load file into memory (for mode 0 and 2). Mode 1 (sendfile) also uses fd.
    char *file_data = NULL; size_t file_size = 0;
    if (use_zerocopy != 1) {
        printf("Loading file into memory...\n");
        if (load_file(filename, &file_data, &file_size) < 0) return 1;
        printf("File size: %.2f MB\n", file_size / (1024.0 * 1024.0));
    } else {
        // We still need the size for header
        struct stat st; if (stat(filename, &st) != 0) { perror("stat"); return 1; }
        file_size = (size_t)st.st_size;
        printf("File size: %.2f MB\n", file_size / (1024.0 * 1024.0));
    }

    // Control connection & header - keep open for all repeats
    int control_sock = connect_to_receiver(receiver_ip, base_port, 4 * 1024 * 1024);
    if (control_sock < 0) { free(file_data); return 1; }

    file_header_t header; header.magic = MAGIC_HEADER; header.file_size = file_size;
    header.repeat_count = repeat_count;
    strncpy(header.filename, strrchr(filename, '/') ? strrchr(filename, '/') + 1 : filename, sizeof(header.filename) - 1);
    header.filename[sizeof(header.filename) - 1] = '\0';

    if (send(control_sock, &header, sizeof(header), 0) != (ssize_t)sizeof(header)) {
        perror("send header"); close(control_sock); free(file_data); return 1;
    }

    printf("Header sent, waiting for receiver ready signal...\n");

    // Wait for initial ready signal
    if (wait_for_ready_signal(control_sock) < 0) {
        printf("Failed to receive initial ready signal\n"); 
        close(control_sock); free(file_data); return 1;
    }

    printf("Receiver ready! Starting data transfers...\n");

    // Accumulate metrics across all repeats
    double total_throughput_sum = 0.0;
    double max_duration_sum = 0.0;
    int total_chunks_sum = 0;

    // Open file for sendfile mode (shared among threads; each uses its own offset)
    int file_fd = -1;
    if (use_zerocopy == 1) {
        file_fd = open(filename, O_RDONLY);
        if (file_fd < 0) { perror("open file for sendfile"); close(control_sock); free(file_data); return 1; }
    }

    for (int repeat = 0; repeat < repeat_count && running; repeat++) {
        printf("\n--- Transfer %d/%d ---\n", repeat + 1, repeat_count);
        
        work_queue_t work_queue;
        if (create_work_queue(&work_queue, file_size) < 0) { 
            if (file_fd >= 0) close(file_fd);
            close(control_sock); free(file_data); return 1; 
        }

        pthread_t threads[num_streams];
        thread_data_t thread_data[num_streams];
        double max_duration = 0.0, total_throughput = 0.0; 
        int total_chunks = 0;

        for (int i = 0; i < num_streams; i++) {
            int port = base_port + 1 + i;
            int sock = connect_to_receiver(receiver_ip, port, 128 * 1024 * 1024);
            if (sock < 0) { printf("Failed to connect stream %d to port %d\n", i, port); return 1; }

            thread_data[i].thread_id = i;
            thread_data[i].socket_fd = sock;
            thread_data[i].file_fd = file_fd;
            thread_data[i].filename = filename;
            thread_data[i].file_data = file_data;
            thread_data[i].total_size = file_size;
            thread_data[i].use_zerocopy = use_zerocopy;
            thread_data[i].work_queue = &work_queue;
            thread_data[i].throughput_mbps = 0.0;
            thread_data[i].duration = 0.0;
            thread_data[i].chunks_processed = 0;

            if (pthread_create(&threads[i], NULL, sender_thread, &thread_data[i]) != 0) {
                perror("pthread_create"); return 1;
            }
        }

        for (int i = 0; i < num_streams; i++) pthread_join(threads[i], NULL);

        for (int i = 0; i < num_streams; i++) {
            total_throughput += thread_data[i].throughput_mbps;
            total_chunks += thread_data[i].chunks_processed;
            if (thread_data[i].duration > max_duration) max_duration = thread_data[i].duration;
        }

        // Accumulate for averaging
        total_throughput_sum += total_throughput;
        max_duration_sum += max_duration;
        total_chunks_sum += total_chunks;

        cleanup_work_queue(&work_queue);

        // Wait for ready signal for next repeat (except after last repeat)
        if (repeat < repeat_count - 1) {
            printf("Waiting for ready signal for next transfer...\n");
            if (wait_for_ready_signal(control_sock) < 0) {
                printf("Failed to receive ready signal for repeat %d, stopping\n", repeat + 2);
                break;
            }
        }
    }

    if (file_fd >= 0) close(file_fd);
    close(control_sock);

    // Calculate and print final averages
    double avg_throughput = total_throughput_sum / repeat_count;
    double avg_duration = max_duration_sum / repeat_count;
    double avg_throughput_gbps = avg_throughput * 8 / 1000.0;

    printf("\n=== FINAL AVERAGE RESULTS (%d transfers) ===\n", repeat_count);
    printf("File size: %.2f MB\n", file_size / (1024.0 * 1024.0));
    printf("Average duration: %.2f seconds\n", avg_duration);
    printf("Average throughput: %.2f MB/s (%.2f Gbps)\n", avg_throughput, avg_throughput_gbps);
    // printf("TX mode: %s\n", use_zerocopy==1?"sendfile":(use_zerocopy==2?"MSG_ZEROCOPY":"writev"));
    // printf("Streams: %d\n", num_streams);

    free(file_data);
    return 0;
}