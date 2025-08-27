// Compile and Run
// gcc -O3 -Wall -pthread -std=gnu11 -o simple_receiver_hpc simple_receiver_hpc.c -lm
// ./simple_receiver_hpc 5303 4 ./ 0

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
#include <sched.h>
#include <math.h>

#define DEFAULT_STREAMS 4
#define DEFAULT_PORT 5303
#define CHUNK_SIZE (32 * 1024 * 1024)  // 32MB chunks
#define MAGIC_HEADER 0xdeadbeefcafebabe

typedef struct {
    uint32_t chunk_id;
    uint64_t file_offset;
    uint32_t chunk_size;
    uint32_t padding;  // For alignment
} chunk_header_t;

typedef struct {
    uint64_t magic;
    uint64_t file_size;
    char filename[256];
} file_header_t;

typedef struct {
    int thread_id;
    int listen_fd;
    int client_fd;
    int port;
    char *file_data;
    size_t total_file_size;
    double throughput_mbps;
    double duration;
    int chunks_received;
    int rx_discard; // if 1, use MSG_TRUNC to discard payload (when save_to_disk=0)
} thread_data_t;

static volatile int running = 1;
static pthread_barrier_t start_barrier;

static inline void print_sockbufs(int fd, const char *tag, int is_rx) {
    int sz = 0; socklen_t sl = sizeof(sz);
    int lev = SOL_SOCKET, opt = is_rx ? SO_RCVBUF : SO_SNDBUF;
    if (getsockopt(fd, lev, opt, &sz, &sl) == 0) {
        printf("%s actual %s buffer: %d bytes\n", tag, is_rx ? "recv" : "send", sz);
    }
    int nodelay = 0; sl = sizeof(nodelay);
    if (getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, &sl) == 0) {
        printf("%s TCP_NODELAY: %s\n", tag, nodelay ? "on" : "off");
    }
}

void signal_handler(int sig) {
    running = 0;
    printf("\nShutdown requested...\n");
}

double get_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

int create_listener(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return -1; }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int buffer_size = 128 * 1024 * 1024;  // request large RX buf
    setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); close(listen_fd); return -1; }
    if (listen(listen_fd, 128) < 0) { perror("listen"); close(listen_fd); return -1; }

    printf("Thread listening on port %d\n", port);
    print_sockbufs(listen_fd, "listen_sock", 1);
    return listen_fd;
}

int set_thread_affinity(int thread_id, int num_cores) {
    cpu_set_t cpuset; CPU_ZERO(&cpuset);
    CPU_SET(thread_id % num_cores, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0) {
        printf("Thread %d bound to CPU %d\n", thread_id, thread_id % num_cores);
        return 0;
    } else { perror("sched_setaffinity"); return -1; }
}

void* receiver_thread(void* arg) {
    thread_data_t *data = (thread_data_t*)arg;

    set_thread_affinity(data->thread_id, 4);

    struct sockaddr_in client_addr; socklen_t client_len = sizeof(client_addr);
    data->client_fd = accept(data->listen_fd, (struct sockaddr*)&client_addr, &client_len);
    if (data->client_fd < 0) { perror("accept"); return NULL; }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    printf("Thread %d: Connection from %s:%d\n", data->thread_id, client_ip, ntohs(client_addr.sin_port));

    int flag = 1;
    setsockopt(data->client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    int buffer_size = 128 * 1024 * 1024;
    setsockopt(data->client_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));
    print_sockbufs(data->client_fd, "data_sock", 1);

    pthread_barrier_wait(&start_barrier);

    double start_time = get_time();
    size_t total_received = 0;
    int chunks_received = 0;

    printf("Thread %d: Ready to receive optimized adaptive chunks (rx_discard=%d)\n",
           data->thread_id, data->rx_discard);

    while (running) {
        chunk_header_t chunk_header;
        ssize_t header_received = 0;
        char *header_ptr = (char*)&chunk_header;
        while (header_received < (ssize_t)sizeof(chunk_header) && running) {
            ssize_t received = recv(data->client_fd, header_ptr + header_received,
                                    sizeof(chunk_header) - header_received, 0);
            if (received < 0) { if (errno == EINTR || errno == EAGAIN) continue; perror("recv chunk_header"); goto thread_exit; }
            else if (received == 0) { printf("Thread %d: Sender closed connection\n", data->thread_id); goto thread_exit; }
            header_received += received;
        }

        if (chunk_header.file_offset + chunk_header.chunk_size > data->total_file_size) {
            printf("Thread %d: Invalid chunk: offset=%lu, size=%u exceeds file size %zu\n",
                   data->thread_id, chunk_header.file_offset, chunk_header.chunk_size, data->total_file_size);
            break;
        }

        // RX fast path: discard in kernel (when not saving)
        if (data->rx_discard) {
            size_t remaining = chunk_header.chunk_size;
            // MSG_TRUNC causes kernel to discard available bytes without copying to user buffer.
            // We probe availability with a small buffer length; kernel drops up to 'remaining'.
            while (remaining > 0 && running) {
                ssize_t dropped = recv(data->client_fd, NULL, remaining, MSG_TRUNC);
                if (dropped < 0) {
                    if (errno == EINTR || errno == EAGAIN) continue;
                    perror("recv MSG_TRUNC"); goto thread_exit;
                } else if (dropped == 0) {
                    printf("Thread %d: Sender closed during MSG_TRUNC\n", data->thread_id);
                    goto thread_exit;
                }
                remaining -= dropped;
                total_received += dropped;
            }
        } else {
            // Normal copy into mapped destination
            char *chunk_buffer = data->file_data + chunk_header.file_offset;
            size_t remaining = chunk_header.chunk_size;
            size_t chunk_received = 0;
            while (remaining > 0 && running) {
                ssize_t received = recv(data->client_fd, chunk_buffer + chunk_received, remaining, 0);
                if (received < 0) { if (errno == EINTR || errno == EAGAIN) continue; perror("recv chunk_data"); goto thread_exit; }
                else if (received == 0) { printf("Thread %d: Sender closed connection during chunk data\n", data->thread_id); goto thread_exit; }
                chunk_received += received;
                remaining -= received;
                total_received += received;
            }
        }
        chunks_received++;
    }

thread_exit: {
        double end_time = get_time();
        double duration = end_time - start_time;
        double throughput = duration > 0 ? (total_received / (1024.0 * 1024.0)) / duration : 0;
        data->throughput_mbps = throughput;
        data->duration = duration;
        data->chunks_received = chunks_received;

        printf("Thread %d: Completed %d chunks, %.2f MB in %.2f s (%.2f MB/s)\n",
               data->thread_id, chunks_received, total_received / (1024.0 * 1024.0), duration, throughput);

        close(data->client_fd);
        close(data->listen_fd);
        return NULL;
    }
}

int save_file(const char *filename, char *data, size_t size) {
    FILE *file = fopen(filename, "wb");
    if (!file) { perror("fopen"); return -1; }
    if (fwrite(data, 1, size, file) != size) { perror("fwrite"); fclose(file); return -1; }
    fclose(file);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 1) {
        printf("Simple High-Performance File Receiver (One-time Transfer)\n");
        printf("Usage: %s [port] [streams] [output_dir] [save_to_disk]\n", argv[0]);
        return 1;
    }

    int base_port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    int num_streams = (argc > 2) ? atoi(argv[2]) : DEFAULT_STREAMS;
    const char *output_dir = (argc > 3) ? argv[3] : "./";
    int save_to_disk = (argc > 4) ? atoi(argv[4]) : 1;

    int repeat_count = 1;
    if (argc > 6) repeat_count = atoi(argv[6]);
    if (repeat_count < 1) repeat_count = 1;

    printf("High-Performance File Receiver (Optimized)\n");
    printf("===========================================\n");
    printf("Base port: %d\n", base_port);
    printf("Streams: %d\n", num_streams);
    printf("Output directory: %s\n", output_dir);
    printf("Save to disk: %s\n", save_to_disk ? "enabled" : "disabled");
    printf("Optimizations: CPU Affinity, Large Buffers, MSG_TRUNC-if-not-saving\n\n");

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    int control_fd = create_listener(base_port);
    if (control_fd < 0) return 1;

    struct sockaddr_in sender_addr; socklen_t sender_len = sizeof(sender_addr);
    printf("Waiting for control connection on port %d...\n", base_port);
    int control_client = accept(control_fd, (struct sockaddr*)&sender_addr, &sender_len);
    if (control_client < 0) { perror("accept control"); close(control_fd); return 1; }

    char sender_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &sender_addr.sin_addr, sender_ip, INET_ADDRSTRLEN);
    printf("Control connection from %s:%d\n", sender_ip, ntohs(sender_addr.sin_port));

    file_header_t header;
    if (recv(control_client, &header, sizeof(header), 0) != (ssize_t)sizeof(header)) {
        perror("recv header"); close(control_client); close(control_fd); return 1;
    }

    if (header.magic != MAGIC_HEADER) {
        printf("Invalid magic header: 0x%lx\n", header.magic);
        close(control_client); close(control_fd); return 1;
    }

    printf("File: %s\n", header.filename);
    printf("Size: %.2f MB\n", header.file_size / (1024.0 * 1024.0));

    char *file_data = NULL;
    if (save_to_disk) {
        file_data = malloc(header.file_size);
        if (!file_data) { perror("malloc file_data"); close(control_client); close(control_fd); return 1; }
    }

    if (pthread_barrier_init(&start_barrier, NULL, num_streams) != 0) {
        perror("pthread_barrier_init"); free(file_data); close(control_client); close(control_fd); return 1;
    }

    pthread_t threads[num_streams];
    thread_data_t thread_data[num_streams];

    for (int i = 0; i < num_streams; i++) {
        int port = base_port + 1 + i;
        int listen_fd = create_listener(port);
        if (listen_fd < 0) return 1;

        thread_data[i].thread_id = i;
        thread_data[i].listen_fd = listen_fd;
        thread_data[i].client_fd = -1;
        thread_data[i].port = port;
        thread_data[i].file_data = file_data;
        thread_data[i].total_file_size = header.file_size;
        thread_data[i].throughput_mbps = 0.0;
        thread_data[i].duration = 0.0;
        thread_data[i].chunks_received = 0;
        thread_data[i].rx_discard = !save_to_disk; // discard when not saving

        if (pthread_create(&threads[i], NULL, receiver_thread, &thread_data[i]) != 0) {
            perror("pthread_create"); return 1;
        }
    }

    printf("Data stream listeners created. Sending ready signal...\n");
    char ready_signal = 'R';
    if (send(control_client, &ready_signal, 1, 0) != 1) {
        perror("send ready signal"); close(control_client); close(control_fd); free(file_data); return 1;
    }

    close(control_client);
    close(control_fd);

    for (int i = 0; i < num_streams; i++) pthread_join(threads[i], NULL);

    double max_duration = 0.0, total_throughput = 0.0; int total_chunks = 0;
    for (int i = 0; i < num_streams; i++) {
        total_throughput += thread_data[i].throughput_mbps;
        total_chunks += thread_data[i].chunks_received;
        if (thread_data[i].duration > max_duration) max_duration = thread_data[i].duration;
    }
    double total_throughput_gbps = total_throughput * 8 / 1000.0;

    printf("\n=== Optimized Adaptive Transfer Complete ===\n");
    printf("Total size: %.2f MB\n", header.file_size / (1024.0 * 1024.0));
    printf("Chunks received: %d\n", total_chunks);
    printf("Duration: %.2f seconds\n", max_duration);
    printf("Total throughput: %.2f MB/s (%.2f Gbps)\n", total_throughput, total_throughput_gbps);
    printf("Optimizations: CPU Affinity, Large Buffers, %s\n", save_to_disk ? "Copy-to-User" : "MSG_TRUNC sink");
    printf("Streams used: %d\n", num_streams);

    if (save_to_disk) {
        char output_path[512];
        snprintf(output_path, sizeof(output_path), "%s/%s", output_dir, header.filename);
        printf("Saving file to: %s\n", output_path);
        if (save_file(output_path, file_data, header.file_size) == 0) printf("File saved successfully!\n");
        else printf("Failed to save file\n");
    } else {
        printf("File not saved (RX used MSG_TRUNC discard mode).\n");
    }

    free(file_data);
    pthread_barrier_destroy(&start_barrier);
    return 0;
}
