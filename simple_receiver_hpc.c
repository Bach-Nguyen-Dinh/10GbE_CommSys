// Compile and Run
// gcc -O3 -Wall -pthread -std=gnu11 -o simple_receiver_hpc simple_receiver_hpc.c -lm
// ./simple_receiver_hpc 5303 4 ./ 0 -r 10

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
    uint32_t padding;  // alignment
} chunk_header_t;

typedef struct {
    uint64_t magic;
    uint64_t file_size;
    char filename[256];
    int repeat_count;  // Add repeat count
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
    int rx_discard; // 1=discard payload instead of saving
    int repeat_num; // Current repeat number
} thread_data_t;

// Global state for repeat coordination
typedef struct {
    pthread_barrier_t start_barrier;
    pthread_barrier_t end_barrier;
    int num_streams;
    int control_client_fd;
    volatile int current_repeat;
    volatile int should_exit;
} repeat_coordinator_t;

static volatile int running = 1;
static repeat_coordinator_t *global_coordinator = NULL;

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
    if (global_coordinator) {
        global_coordinator->should_exit = 1;
    }
}

double get_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

int create_listener(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return -1; }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int buffer_size = 128 * 1024 * 1024;
    setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); close(listen_fd); return -1; }
    if (listen(listen_fd, 128) < 0) { perror("listen"); close(listen_fd); return -1; }

    printf("Thread listening on port %d\n", port);
    return listen_fd;
}

int set_thread_affinity(int thread_id, int num_cores) {
    cpu_set_t cpuset; CPU_ZERO(&cpuset);
    CPU_SET(thread_id % num_cores, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0) {
        return 0;
    } else { perror("sched_setaffinity"); return -1; }
}

int send_ready_signal(int control_fd) {
    char ready = 'R';
    ssize_t sent = send(control_fd, &ready, 1, 0);
    return (sent == 1) ? 0 : -1;
}

void* receiver_thread(void* arg) {
    thread_data_t *data = (thread_data_t*)arg;
    set_thread_affinity(data->thread_id, 4);

    struct sockaddr_in client_addr; socklen_t client_len = sizeof(client_addr);
    data->client_fd = accept(data->listen_fd, (struct sockaddr*)&client_addr, &client_len);
    if (data->client_fd < 0) { perror("accept"); return NULL; }

    int flag = 1;
    setsockopt(data->client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    int buffer_size = 128 * 1024 * 1024;
    setsockopt(data->client_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

    // Reset per-repeat stats
    data->throughput_mbps = 0.0;
    data->duration = 0.0;
    data->chunks_received = 0;

    pthread_barrier_wait(&global_coordinator->start_barrier);

    double start_time = get_time();
    size_t total_received = 0;
    int chunks_received = 0;

    while (running && !global_coordinator->should_exit) {
        chunk_header_t chunk_header;
        ssize_t header_received = 0;
        char *header_ptr = (char*)&chunk_header;
        while (header_received < (ssize_t)sizeof(chunk_header) && running) {
            ssize_t received = recv(data->client_fd, header_ptr + header_received,
                                    sizeof(chunk_header) - header_received, 0);
            if (received < 0) { 
                if (errno == EINTR || errno == EAGAIN) continue; 
                perror("recv chunk_header"); 
                goto thread_exit; 
            }
            else if (received == 0) { 
                // Sender closed connection - this is normal at end of transfer
                goto thread_exit; 
            }
            header_received += received;
        }

        if (chunk_header.file_offset + chunk_header.chunk_size > data->total_file_size) {
            printf("Thread %d: Invalid chunk: offset=%lu, size=%u exceeds file size %zu\n",
                   data->thread_id, chunk_header.file_offset, chunk_header.chunk_size, data->total_file_size);
            break;
        }

        if (data->rx_discard) {
            size_t remaining = chunk_header.chunk_size;
            while (remaining > 0 && running) {
                ssize_t dropped = recv(data->client_fd, NULL, remaining, MSG_TRUNC);
                if (dropped < 0) { if (errno == EINTR || errno == EAGAIN) continue; perror("recv MSG_TRUNC"); goto thread_exit; }
                else if (dropped == 0) { goto thread_exit; }
                remaining -= dropped;
                total_received += dropped;
            }
        } else {
            char *chunk_buffer = data->file_data + chunk_header.file_offset;
            size_t remaining = chunk_header.chunk_size;
            size_t chunk_received = 0;
            while (remaining > 0 && running) {
                ssize_t received = recv(data->client_fd, chunk_buffer + chunk_received, remaining, 0);
                if (received < 0) { if (errno == EINTR || errno == EAGAIN) continue; perror("recv chunk_data"); goto thread_exit; }
                else if (received == 0) { goto thread_exit; }
                chunk_received += received;
                remaining -= received;
                total_received += received;
            }
        }
        chunks_received++;
    }

thread_exit:
    {
        double end_time = get_time();
        double duration = end_time - start_time;
        double throughput = duration > 0 ? (total_received / (1024.0 * 1024.0)) / duration : 0;
        data->throughput_mbps = throughput;
        data->duration = duration;
        data->chunks_received = chunks_received;

        printf("Thread %d: Transfer %d completed: %d chunks, %.2f MB in %.2f s (%.2f MB/s)\n",
               data->thread_id, data->repeat_num, chunks_received, 
               total_received / (1024.0 * 1024.0), duration, throughput);

        close(data->client_fd);
        
        // Wait for all threads to complete this transfer
        pthread_barrier_wait(&global_coordinator->end_barrier);
        
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
    if (argc < 1) return 1;

    int base_port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    int num_streams = (argc > 2) ? atoi(argv[2]) : DEFAULT_STREAMS;
    const char *output_dir = (argc > 3) ? argv[3] : "./";
    int save_to_disk = (argc > 4) ? atoi(argv[4]) : 1;

    printf("High-Performance File Receiver (Repeat Support)\n");
    printf("Base port: %d, Streams: %d, Output dir: %s, Save: %s\n",
           base_port, num_streams, output_dir, save_to_disk ? "yes" : "no");

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    int control_fd = create_listener(base_port);
    if (control_fd < 0) return 1;

    struct sockaddr_in sender_addr; socklen_t sender_len = sizeof(sender_addr);
    int control_client = accept(control_fd, (struct sockaddr*)&sender_addr, &sender_len);
    if (control_client < 0) { perror("accept control"); close(control_fd); return 1; }

    file_header_t header;
    if (recv(control_client, &header, sizeof(header), 0) != (ssize_t)sizeof(header)) {
        perror("recv header"); close(control_client); close(control_fd); return 1;
    }

    if (header.magic != MAGIC_HEADER) {
        printf("Invalid magic header: 0x%lx\n", header.magic);
        close(control_client); close(control_fd); return 1;
    }

    int repeat_count = header.repeat_count;
    if (repeat_count < 1) repeat_count = 1;

    printf("Receiving file: %s, size: %.2f MB, repeats: %d\n", 
           header.filename, header.file_size / (1024.0*1024.0), repeat_count);

    char *file_data = NULL;
    if (save_to_disk) {
        file_data = malloc(header.file_size);
        if (!file_data) { perror("malloc"); close(control_client); close(control_fd); return 1; }
    }

    // Initialize repeat coordinator
    repeat_coordinator_t coordinator;
    global_coordinator = &coordinator;
    coordinator.num_streams = num_streams;
    coordinator.control_client_fd = control_client;
    coordinator.current_repeat = 0;
    coordinator.should_exit = 0;

    if (pthread_barrier_init(&coordinator.start_barrier, NULL, num_streams) != 0) {
        perror("pthread_barrier_init start"); 
        free(file_data); close(control_client); close(control_fd); return 1;
    }
    if (pthread_barrier_init(&coordinator.end_barrier, NULL, num_streams + 1) != 0) {
        perror("pthread_barrier_init end"); 
        pthread_barrier_destroy(&coordinator.start_barrier);
        free(file_data); close(control_client); close(control_fd); return 1;
    }

    // Create listeners for data streams
    int listen_fds[num_streams];
    for (int i = 0; i < num_streams; i++) {
        int port = base_port + 1 + i;
        listen_fds[i] = create_listener(port);
        if (listen_fds[i] < 0) return 1;
    }

    // Accumulate metrics across all repeats
    double total_throughput_sum = 0.0;
    double max_duration_sum = 0.0;
    int total_chunks_sum = 0;

    // Send initial ready signal
    if (send_ready_signal(control_client) < 0) {
        printf("Failed to send initial ready signal\n");
        goto cleanup;
    }

    for (int repeat = 0; repeat < repeat_count && running && !coordinator.should_exit; repeat++) {
        printf("\n--- Receiving transfer %d/%d ---\n", repeat + 1, repeat_count);
        coordinator.current_repeat = repeat;

        pthread_t threads[num_streams];
        thread_data_t thread_data[num_streams];

        for (int i = 0; i < num_streams; i++) {
            thread_data[i].thread_id = i;
            thread_data[i].listen_fd = listen_fds[i];
            thread_data[i].client_fd = -1;
            thread_data[i].port = base_port + 1 + i;
            thread_data[i].file_data = file_data;
            thread_data[i].total_file_size = header.file_size;
            thread_data[i].throughput_mbps = 0.0;
            thread_data[i].duration = 0.0;
            thread_data[i].chunks_received = 0;
            thread_data[i].rx_discard = !save_to_disk;
            thread_data[i].repeat_num = repeat + 1;

            if (pthread_create(&threads[i], NULL, receiver_thread, &thread_data[i]) != 0) {
                perror("pthread_create"); return 1;
            }
        }

        // Wait for all threads to complete this transfer
        pthread_barrier_wait(&coordinator.end_barrier);

        for (int i = 0; i < num_streams; i++) {
            pthread_join(threads[i], NULL);
        }

        // Accumulate metrics
        double max_duration = 0.0, total_throughput = 0.0;
        int total_chunks = 0;
        for (int i = 0; i < num_streams; i++) {
            total_throughput += thread_data[i].throughput_mbps;
            total_chunks += thread_data[i].chunks_received;
            if (thread_data[i].duration > max_duration) max_duration = thread_data[i].duration;
        }

        total_throughput_sum += total_throughput;
        max_duration_sum += max_duration;
        total_chunks_sum += total_chunks;

        // Send ready signal for next transfer (except after last transfer)
        if (repeat < repeat_count - 1) {
            printf("Sending ready signal for next transfer...\n");
            if (send_ready_signal(control_client) < 0) {
                printf("Failed to send ready signal for repeat %d\n", repeat + 2);
                break;
            }
        }
    }
    printf("\nAll transfers completed or interrupted.\n");

    // Calculate and print final averages
    double avg_throughput = total_throughput_sum / repeat_count;
    double avg_duration = max_duration_sum / repeat_count;
    double avg_throughput_gbps = avg_throughput * 8 / 1000.0;

    printf("\n=== FINAL AVERAGE RESULTS (%d transfers) ===\n", repeat_count);
    printf("File size: %.2f MB\n", header.file_size / (1024.0 * 1024.0));
    printf("Average duration: %.2f seconds\n", avg_duration);
    printf("Average throughput: %.2f MB/s (%.2f Gbps)\n", avg_throughput, avg_throughput_gbps);

    if (save_to_disk) {
        char path[512]; 
        snprintf(path, sizeof(path), "%s/%s", output_dir, header.filename);
        if (save_file(path, file_data, header.file_size) == 0) {
            printf("Final file saved: %s\n", path);
        } else {
            printf("Failed to save final file\n");
        }
    }

cleanup:
    close(control_client);
    close(control_fd);
    for (int i = 0; i < num_streams; i++) {
        close(listen_fds[i]);
    }
    free(file_data);
    pthread_barrier_destroy(&coordinator.start_barrier);
    pthread_barrier_destroy(&coordinator.end_barrier);
    return 0;
}