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
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sched.h>
#include <signal.h>
#include <endian.h>

#define DEFAULT_PORT 5303
#define DEFAULT_STREAMS 4
#define BUFFER_SIZE (2 * 1024 * 1024)  // 2MB buffer
#define SOCKET_BUFFER_SIZE (16 * 1024 * 1024)  // 16MB socket buffer
#define MAX_FILES 10  // Default number of files to receive
#define MAX_FILENAME_LEN 256
#define SYNC_MAGIC 0xDEADBEEFCAFEBABE  // Must match sender

#define ADAPTIVE_CHUNK_SIZE (16 * 1024 * 1024)  // 16MB adaptive chunks
#define MAX_WORK_ITEMS 1024  // Maximum work queue size
#define CMD_PORT 5400

// Performance tracking for adaptive sizing
typedef struct {
    double throughput_mbps;
    double avg_chunk_time;
    size_t chunks_completed;
    double last_update_time;
    double performance_factor;  // Relative performance (1.0 = average)
} thread_performance_t;

// Work queue for dynamic load balancing
typedef struct {
    size_t offset;
    size_t size;
    int assigned_thread;  // -1 if unassigned
} work_item_t;

// Adaptive context (add to both sender_ctx_t and receiver_ctx_t)
typedef struct {
    pthread_mutex_t work_mutex;
    work_item_t work_queue[MAX_WORK_ITEMS];
    int work_head;  // Next item to assign
    int work_tail;  // Last item in queue
    int total_work_items;
    
    // Performance tracking
    thread_performance_t thread_perf[32];  // Support up to 32 threads
    double global_avg_throughput;
    int adaptation_enabled;
    
    // Current file context
    size_t current_file_size;
    size_t total_bytes_processed;
    double file_start_time;
} adaptive_ctx_t;

typedef struct {
    int thread_id;
    int listen_fd;
    int client_fd;
    char *buffer;
    size_t bytes_received;
    int port;
    int num_streams;
    pthread_mutex_t *stats_mutex;
    pthread_barrier_t *file_barrier;
    size_t expected_file_size;
    char filename[MAX_FILENAME_LEN];
    double *file_start_times;
    double *file_end_times;
    size_t *file_bytes_received;
    void *global_ctx;

    // Adaptive chunk fields
    size_t current_chunk_offset;
    size_t current_chunk_size;
    size_t total_bytes_processed;
    double chunk_start_time;
    double last_performance_update;
    
    // Performance tracking
    double recent_throughput[5];  // Rolling average
    int perf_sample_count;
    
    // Adaptive context pointer
    adaptive_ctx_t *adaptive_ctx;

    volatile int ready_for_header;  // Thread 0 sets this when ready
} thread_data_t;

typedef struct {
    size_t total_received;
    double overall_start_time;
    double overall_end_time;
    int num_streams;
    int max_files;
    pthread_t *threads;
    thread_data_t *thread_data;
    pthread_mutex_t stats_mutex;
    pthread_barrier_t file_barrier;
    volatile int files_completed;
    volatile int test_active;
    size_t expected_file_size;
    char current_filename[MAX_FILENAME_LEN];
    double total_throughput;
    int command_socket;
    int command_port;
} receiver_ctx_t;

static volatile int running = 1;

// Function Prototypes
static double get_time(void);
static void signal_handler(int sig);
static int send_all(int sockfd, const void *buf, size_t len);
static int recv_all(int sockfd, void *buf, size_t len);
static int setup_socket_options(int sock);
static int create_listener(int port);
static void set_thread_affinity(int thread_id);
static int receive_file_header(int sock, size_t *file_size, int *num_streams, char *filename, int expected_file_num);
static void *adaptive_receiver_thread(void *arg);
void cleanup_adaptive_context(adaptive_ctx_t *ctx);
int init_adaptive_context(adaptive_ctx_t *ctx, size_t file_size, int num_threads);
int get_next_work_item(adaptive_ctx_t *ctx, int thread_id, size_t *offset, size_t *size);
void update_thread_performance(adaptive_ctx_t *ctx, int thread_id, size_t bytes_processed, double duration);
static int connect_to_sender(const char *sender_ip, int port);
static int setup_simple_sync_receiver(receiver_ctx_t *ctx, int command_port);
static void signal_file_complete(receiver_ctx_t *ctx, int file_num);

static double get_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void signal_handler(int sig) {
    printf("\nReceived signal %d, shutting down gracefully...\n", sig);
    running = 0;
}

// Robust send - ensures all bytes are sent
static int send_all(int sockfd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;
    
    while (total_sent < len && running) {
        ssize_t sent = send(sockfd, ptr + total_sent, len - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (sent == 0 || (errno != EINTR && errno != EAGAIN)) {
                return -1;
            }
            continue;
        }
        total_sent += sent;
    }
    return (total_sent == len) ? 0 : -1;
}

// Robust receive - ensures all bytes are received  
static int recv_all(int sockfd, void *buf, size_t len) {
    size_t total_received = 0;
    char *ptr = (char *)buf;
    
    while (total_received < len && running) {
        ssize_t received = recv(sockfd, ptr + total_received, len - total_received, 0);
        if (received <= 0) {
            if (received == 0 || errno == ECONNRESET) {
                printf("Connection closed during recv_all (received %zu/%zu bytes)\n", 
                       total_received, len);
                return -1;
            }
            if (errno != EINTR && errno != EAGAIN) {
                perror("recv_all");
                return -1;
            }
            continue;
        }
        total_received += received;
    }
    return (total_received == len) ? 0 : -1;
}

static int setup_socket_options(int sock) {
    int opt = 1;
    int buffer_size = SOCKET_BUFFER_SIZE;
    
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        return -1;
    }
    
    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        perror("setsockopt TCP_NODELAY");
        return -1;
    }
    
    if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        perror("setsockopt SO_RCVBUF");
        return -1;
    }
    
    return 0;
}

static int create_listener(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return -1;
    }
    
    if (setup_socket_options(sock) < 0) {
        close(sock);
        return -1;
    }
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(sock);
        return -1;
    }
    
    if (listen(sock, 128) < 0) {
        perror("listen");
        close(sock);
        return -1;
    }
    
    printf("Thread %d listening on port %d\n", port - DEFAULT_PORT, port);
    return sock;
}

static void set_thread_affinity(int thread_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    
    int num_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    int cpu_id = thread_id % num_cpus;
    
    CPU_SET(cpu_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
        perror("pthread_setaffinity_np");
    } else {
        printf("Thread %d bound to CPU %d\n", thread_id, cpu_id);
    }
}

static int receive_file_header(int sock, size_t *file_size, int *num_streams, char *filename, int expected_file_num) {
    // Read synchronization marker
    uint64_t net_sync_marker;
    if (recv_all(sock, &net_sync_marker, sizeof(net_sync_marker)) < 0) {
        printf("Failed to receive sync marker\n");
        return -1;
    }
    
    uint64_t sync_marker = be64toh(net_sync_marker);
    if (sync_marker != SYNC_MAGIC) {
        printf("SYNC ERROR: Expected 0x%lx, got 0x%lx\n", SYNC_MAGIC, sync_marker);
        return -1;
    }
    
    // Read file number
    uint32_t net_file_num;
    if (recv_all(sock, &net_file_num, sizeof(net_file_num)) < 0) {
        printf("Failed to receive file number\n");
        return -1;
    }
    
    uint32_t file_num = ntohl(net_file_num);
    if ((int)file_num != expected_file_num) {
        printf("FILE NUM ERROR: Expected %d, got %u\n", expected_file_num, file_num);
        return -1;
    }
    
    // Read file size
    uint64_t net_file_size;
    if (recv_all(sock, &net_file_size, sizeof(net_file_size)) < 0) {
        printf("Failed to receive file size\n");
        return -1;
    }
    *file_size = be64toh(net_file_size);
    
    // Sanity check file size (should be reasonable)
    if (*file_size == 0 || *file_size > (10ULL * 1024 * 1024 * 1024)) { // Max 10GB
        printf("INVALID FILE SIZE: %zu bytes\n", *file_size);
        return -1;
    }
    
    // Read number of streams
    uint32_t net_num_streams;
    if (recv_all(sock, &net_num_streams, sizeof(net_num_streams)) < 0) {
        printf("Failed to receive stream count\n");
        return -1;
    }
    *num_streams = ntohl(net_num_streams);
    
    // Sanity check stream count
    if (*num_streams < 1 || *num_streams > 16) {
        printf("INVALID STREAM COUNT: %d\n", *num_streams);
        return -1;
    }
    
    // Read filename length
    uint32_t net_filename_len;
    if (recv_all(sock, &net_filename_len, sizeof(net_filename_len)) < 0) {
        printf("Failed to receive filename length\n");
        return -1;
    }
    uint32_t filename_len = ntohl(net_filename_len);
    
    // Sanity check filename length
    if (filename_len >= MAX_FILENAME_LEN) {
        printf("FILENAME TOO LONG: %u bytes\n", filename_len);
        return -1;
    }
    
    // Read filename
    if (filename_len > 0) {
        if (recv_all(sock, filename, filename_len) < 0) {
            printf("Failed to receive filename\n");
            return -1;
        }
        filename[filename_len] = '\0';
    } else {
        strcpy(filename, "unknown");
    }
    
    printf("Thread 0: Received valid header for file %d (sync=0x%lx, size=%zu, filename='%s')\n", 
           expected_file_num, sync_marker, *file_size, filename);
    return 0;
}

static void *adaptive_receiver_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    receiver_ctx_t *ctx = (receiver_ctx_t *)data->global_ctx;
    
    set_thread_affinity(data->thread_id);
    
    // Allocate receive buffer
    data->buffer = malloc(BUFFER_SIZE);
    if (!data->buffer) {
        perror("malloc buffer");
        return NULL;
    }
    
    printf("Thread %d ready, waiting for connection...\n", data->thread_id);
    
    // Accept connection (existing code - KEEP THIS)
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);
    data->client_fd = accept(data->listen_fd, (struct sockaddr *)&client_addr, &addr_len);
    if (data->client_fd < 0) {
        perror("accept");
        free(data->buffer);
        return NULL;
    }
    
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    printf("Thread %d: Persistent connection from %s:%d\n", 
           data->thread_id, client_ip, ntohs(client_addr.sin_port));
    
    setup_socket_options(data->client_fd);
    
    // COMPLETELY REPLACE THE FILE PROCESSING LOOP (lines 365-500)
    for (int file_num = 1; file_num <= ctx->max_files && running && ctx->test_active; file_num++) {
        data->bytes_received = 0;
        data->total_bytes_processed = 0;
        
        // Only thread 0 handles synchronization and headers
        if (data->thread_id == 0) {
            // Wait for sender sync request and send ready signal
            signal_file_complete(ctx, file_num);
            
            // Receive file header
            size_t file_size;
            int num_streams;
            char filename[MAX_FILENAME_LEN];
            
            if (receive_file_header(data->client_fd, &file_size, &num_streams, filename, file_num) < 0) {
                printf("Thread 0: Failed to receive valid header for file %d\n", file_num);
                ctx->test_active = 0;
                pthread_barrier_wait(data->file_barrier);
                break;
            }
            
            // Set global file info
            ctx->expected_file_size = file_size;
            strcpy(ctx->current_filename, filename);
            
            printf("=== FILE %d/%d ===\n", file_num, ctx->max_files);  
            printf("File: %s, Size: %.2f MB, Streams: %d\n", 
                   filename, file_size / (1024.0 * 1024.0), num_streams);
            
            // Initialize adaptive context for this file
            if (init_adaptive_context(data->adaptive_ctx, file_size, data->num_streams) < 0) {
                printf("Failed to initialize adaptive receive context\n");
                ctx->test_active = 0;
                pthread_barrier_wait(data->file_barrier);
                break;
            }
            
            printf("Thread 0: Header received successfully for file %d\n", file_num);
        }
        
        // BARRIER 1: Wait for header setup
        pthread_barrier_wait(data->file_barrier);
        
        if (!ctx->test_active) {
            printf("Thread %d: Test marked inactive, exiting\n", data->thread_id);
            break;
        }
        
        // Set file info for all threads
        data->expected_file_size = ctx->expected_file_size;
        strcpy(data->filename, ctx->current_filename);
        
        // Record file start time
        data->file_start_times[file_num - 1] = get_time();
        
        // ADAPTIVE CHUNK RECEIVING LOOP (KEEP THIS LOGIC)
        size_t chunk_offset, chunk_size;
        while (get_next_work_item(data->adaptive_ctx, data->thread_id, &chunk_offset, &chunk_size) && 
               running && ctx->test_active) {
            
            double chunk_start = get_time();
            
            // Receive this chunk
            size_t bytes_remaining = chunk_size;
            size_t bytes_received_this_chunk = 0;
            
            while (bytes_remaining > 0 && running && ctx->test_active) {
                size_t bytes_to_receive = (bytes_remaining > BUFFER_SIZE) ? BUFFER_SIZE : bytes_remaining;
                
                ssize_t bytes = recv(data->client_fd, data->buffer, bytes_to_receive, 0);
                if (bytes <= 0) {
                    if (bytes == 0) {
                        printf("Thread %d: Connection closed by sender\n", data->thread_id);
                    } else {
                        printf("Thread %d: Receive error: %s\n", data->thread_id, strerror(errno));
                    }
                    ctx->test_active = 0;
                    goto chunk_loop_exit;
                }
                
                data->bytes_received += bytes;
                bytes_received_this_chunk += bytes;
                bytes_remaining -= bytes;
                data->total_bytes_processed += bytes;
            }
            
            if (!ctx->test_active) break;
            
            // Update performance metrics
            double chunk_duration = get_time() - chunk_start;
            if (chunk_duration > 0) {
                update_thread_performance(data->adaptive_ctx, data->thread_id, 
                                         bytes_received_this_chunk, chunk_duration);
            }
        }
        
chunk_loop_exit:
        // Record file end time
        data->file_end_times[file_num - 1] = get_time();
        data->file_bytes_received[file_num - 1] = data->bytes_received;
        
        // BARRIER 2: Wait for all data receiving to complete
        pthread_barrier_wait(data->file_barrier);
        
        if (!ctx->test_active) break;
        
        // THREAD 0: FILE COMPLETION REPORTING (KEEP THIS)
        if (data->thread_id == 0) {
            // Calculate file statistics
            double min_start = data->file_start_times[file_num - 1];
            double max_end = data->file_end_times[file_num - 1];
            size_t file_total = 0;
            
            // Find actual min/max times across all threads
            for (int i = 0; i < data->num_streams; i++) {
                if (ctx->thread_data[i].file_start_times[file_num - 1] < min_start) {
                    min_start = ctx->thread_data[i].file_start_times[file_num - 1];
                }
                if (ctx->thread_data[i].file_end_times[file_num - 1] > max_end) {
                    max_end = ctx->thread_data[i].file_end_times[file_num - 1];
                }
                file_total += ctx->thread_data[i].file_bytes_received[file_num - 1];
            }
            
            double total_duration = max_end - min_start;
            double total_throughput = (file_total * 8.0) / (total_duration * 1e9);
            
            printf("FILE %d COMPLETE: %.2f MB in %.3f sec (%.2f Gbps)\n", 
                   file_num, file_total / (1024.0 * 1024.0), total_duration, total_throughput);
            
            // Update global stats
            pthread_mutex_lock(data->stats_mutex);
            ctx->total_received += file_total;
            ctx->total_throughput += total_throughput;
            ctx->files_completed = file_num;  // IMPORTANT: Update this correctly
            pthread_mutex_unlock(data->stats_mutex);
            
            printf("Files completed: %d/%d\n", ctx->files_completed, ctx->max_files);
            printf("----------------------------------------\n");
        }
        
        // BARRIER 3: Synchronize before next file
        pthread_barrier_wait(data->file_barrier);
    }
    
    close(data->client_fd);
    free(data->buffer);
    printf("Thread %d completed all files\n", data->thread_id);
    return NULL;
}


void cleanup_adaptive_context(adaptive_ctx_t *ctx) {
    pthread_mutex_destroy(&ctx->work_mutex);
}

// Initialize adaptive context
int init_adaptive_context(adaptive_ctx_t *ctx, size_t file_size, int num_threads) {
    memset(ctx, 0, sizeof(adaptive_ctx_t));
    
    if (pthread_mutex_init(&ctx->work_mutex, NULL) != 0) {
        return -1;
    }
    
    ctx->current_file_size = file_size;
    ctx->adaptation_enabled = 1;
    ctx->file_start_time = get_time();
    
    // Initialize thread performance tracking
    for (int i = 0; i < num_threads; i++) {
        ctx->thread_perf[i].performance_factor = 1.0;  // Start equal
        ctx->thread_perf[i].last_update_time = ctx->file_start_time;
    }
    
    // Create work queue - divide file into adaptive chunks
    size_t chunk_size = ADAPTIVE_CHUNK_SIZE;
    size_t offset = 0;
    int work_item = 0;
    
    while (offset < file_size && work_item < MAX_WORK_ITEMS) {
        ctx->work_queue[work_item].offset = offset;
        ctx->work_queue[work_item].size = (offset + chunk_size > file_size) ? 
                                          (file_size - offset) : chunk_size;
        ctx->work_queue[work_item].assigned_thread = -1;
        
        offset += ctx->work_queue[work_item].size;
        work_item++;
    }
    
    ctx->total_work_items = work_item;
    ctx->work_head = 0;
    ctx->work_tail = work_item - 1;
    
    printf("Adaptive: Created %d work items of %zu MB each\n", 
           work_item, chunk_size / (1024 * 1024));
    
    return 0;
}

// Get next work item for a thread
int get_next_work_item(adaptive_ctx_t *ctx, int thread_id, size_t *offset, size_t *size) {
    pthread_mutex_lock(&ctx->work_mutex);
    
    if (ctx->work_head > ctx->work_tail) {
        // No more work
        pthread_mutex_unlock(&ctx->work_mutex);
        return 0;
    }
    
    // Assign work item to this thread
    work_item_t *item = &ctx->work_queue[ctx->work_head];
    item->assigned_thread = thread_id;
    
    *offset = item->offset;
    *size = item->size;
    
    ctx->work_head++;
    
    printf("Thread %d: Got work item %d: offset=%zu, size=%zu MB\n", 
           thread_id, ctx->work_head - 1, *offset, *size / (1024 * 1024));
    
    pthread_mutex_unlock(&ctx->work_mutex);
    return 1;
}

// Update thread performance metrics
void update_thread_performance(adaptive_ctx_t *ctx, int thread_id, size_t bytes_processed, double duration) {
    if (duration <= 0) return;
    
    pthread_mutex_lock(&ctx->work_mutex);
    
    thread_performance_t *perf = &ctx->thread_perf[thread_id];
    
    // Calculate current throughput (MB/s)
    double current_throughput = (bytes_processed / (1024.0 * 1024.0)) / duration;
    
    // Update rolling average
    perf->chunks_completed++;
    if (perf->chunks_completed == 1) {
        perf->throughput_mbps = current_throughput;
    } else {
        // Exponential moving average (alpha = 0.3)
        perf->throughput_mbps = 0.3 * current_throughput + 0.7 * perf->throughput_mbps;
    }
    
    perf->avg_chunk_time = duration;
    perf->last_update_time = get_time();
    
    // Calculate global average
    double total_throughput = 0;
    int active_threads = 0;
    for (int i = 0; i < 32; i++) {
        if (ctx->thread_perf[i].chunks_completed > 0) {
            total_throughput += ctx->thread_perf[i].throughput_mbps;
            active_threads++;
        }
    }
    
    if (active_threads > 0) {
        ctx->global_avg_throughput = total_throughput / active_threads;
        
        // Update performance factors
        for (int i = 0; i < active_threads; i++) {
            if (ctx->thread_perf[i].chunks_completed > 0) {
                ctx->thread_perf[i].performance_factor = 
                    ctx->thread_perf[i].throughput_mbps / ctx->global_avg_throughput;
            }
        }
    }
    
    ctx->total_bytes_processed += bytes_processed;
    
    pthread_mutex_unlock(&ctx->work_mutex);
    
    printf("Thread %d: %.2f MB/s (factor: %.2f, global avg: %.2f MB/s)\n", 
           thread_id, current_throughput, perf->performance_factor, ctx->global_avg_throughput);
}

static int setup_simple_sync_receiver(receiver_ctx_t *ctx, int command_port) {
    struct sockaddr_in addr;
    int opt = 1;
    
    // Create simple command socket
    ctx->command_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->command_socket < 0) {
        perror("command socket");
        return -1;
    }
    
    setsockopt(ctx->command_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(command_port);
    
    if (bind(ctx->command_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("command bind");
        close(ctx->command_socket);
        return -1;
    }
    
    if (listen(ctx->command_socket, 1) < 0) {
        perror("command listen");
        close(ctx->command_socket);
        return -1;
    }
    
    printf("Simple sync receiver listening on port %d\n", command_port);
    return 0;
}

static void signal_file_complete(receiver_ctx_t *ctx, int file_num) {
    int client_fd;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    char ready_msg = 'R';  // Simple ready signal
    
    printf("Waiting for sender sync request for file %d...\n", file_num + 1);
    
    // Accept connection from sender
    client_fd = accept(ctx->command_socket, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd < 0) {
        perror("command accept");
        return;
    }
    
    // Send simple ready signal
    if (send(client_fd, &ready_msg, 1, 0) != 1) {
        perror("send ready signal");
    } else {
        printf("Sent ready signal for file %d\n", file_num + 1);
    }
    
    close(client_fd);
}

static int connect_to_sender(const char *sender_ip, int port) {
int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return -1;
    }
    
    // === PREVENTION LAYER: Enable comprehensive socket health monitoring ===
    
    // Enable TCP keepalive to detect broken connections
    int keepalive = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) < 0) {
        perror("setsockopt SO_KEEPALIVE");
    }
    
    // Aggressive keepalive settings for quick detection
    int keepidle = 20;    // Start probing after 20 seconds of idle
    int keepintvl = 3;    // Probe every 3 seconds
    int keepcnt = 3;      // Fail after 3 failed probes
    
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle));
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl));
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt));
    
    // Low-latency signaling options
    int nodelay = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    
    // Optimal buffer sizes for signaling
    int sndbuf = 4096, rcvbuf = 4096;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    
    // Connection setup
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, sender_ip, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sock);
        return -1;
    }
    
    // Retry connection with exponential backoff
    for (int retry = 0; retry < 10; retry++) {
        if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            printf("Robust ready signaling connection established on port %d\n", port);
            return sock;
        }
        
        if (retry < 9) {
            int wait_time = (1 << retry) > 8 ? 8 : (1 << retry);  // Cap at 8 seconds
            printf("Connection failed, retrying in %d seconds... (%d/10)\n", wait_time, retry + 1);
            sleep(wait_time);
        }
    }
    
    printf("Failed to establish robust ready signaling connection\n");
    close(sock);
    return -1;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s [port] [streams] [max_files] [sender_ip]\n", argv[0]);
        printf("Example: %s 5303 4 10 169.254.207.123\n", argv[0]);
        return 1;
    }
    
    int base_port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    int num_streams = (argc > 2) ? atoi(argv[2]) : DEFAULT_STREAMS;
    int max_files = (argc > 3) ? atoi(argv[3]) : MAX_FILES;
    const char *sender_ip = (argc > 4) ? argv[4] : "169.254.207.123";  // Default sender IP
    
    printf("Robust High-Performance File Receiver (Adaptive Connections)\n");
    printf("==============================================================\n");
    printf("Base port: %d\n", base_port);
    printf("Streams: %d\n", num_streams);
    printf("Max files: %d\n", max_files);
    printf("Sender IP: %s\n", sender_ip);
    printf("Mode: File-count based with Adaptive TCP chunking\n");
    printf("Press Ctrl+C to shutdown\n\n");
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    receiver_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.num_streams = num_streams;
    ctx.max_files = max_files;
    ctx.test_active = 1;
    ctx.files_completed = 0;
    ctx.total_received = 0;
    ctx.total_throughput = 0;
    ctx.command_port = CMD_PORT;

    if (setup_simple_sync_receiver(&ctx, ctx.command_port) < 0) {
        return 1;
    }

    // Allocate and initialize adaptive context
    adaptive_ctx_t adaptive_ctx;
    memset(&adaptive_ctx, 0, sizeof(adaptive_ctx));  // Initialize to zero
    
    ctx.threads = malloc(num_streams * sizeof(pthread_t));
    ctx.thread_data = malloc(num_streams * sizeof(thread_data_t));
    
    if (pthread_mutex_init(&ctx.stats_mutex, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }
    
    if (pthread_barrier_init(&ctx.file_barrier, NULL, num_streams) != 0) {
        perror("pthread_barrier_init");
        return 1;
    }
    
    // Create listeners and start threads
    for (int i = 0; i < num_streams; i++) {
        int port = base_port + i;
        int listen_fd = create_listener(port);
        if (listen_fd < 0) {
            return 1;
        }
        
        // Initialize thread data structure
        memset(&ctx.thread_data[i], 0, sizeof(thread_data_t));  // Clear everything first
        
        ctx.thread_data[i].thread_id = i;
        ctx.thread_data[i].listen_fd = listen_fd;
        ctx.thread_data[i].client_fd = -1;
        ctx.thread_data[i].port = port;
        ctx.thread_data[i].bytes_received = 0;
        ctx.thread_data[i].num_streams = num_streams;
        ctx.thread_data[i].stats_mutex = &ctx.stats_mutex;
        ctx.thread_data[i].file_barrier = &ctx.file_barrier;
        ctx.thread_data[i].global_ctx = &ctx;
        ctx.thread_data[i].adaptive_ctx = &adaptive_ctx;
        ctx.thread_data[i].current_chunk_offset = 0;
        ctx.thread_data[i].current_chunk_size = 0;
        ctx.thread_data[i].total_bytes_processed = 0;
        ctx.thread_data[i].chunk_start_time = 0.0;
        ctx.thread_data[i].last_performance_update = 0.0;
        ctx.thread_data[i].perf_sample_count = 0;
        
        // Initialize performance tracking arrays
        for (int j = 0; j < 5; j++) {
            ctx.thread_data[i].recent_throughput[j] = 0.0;
        }

        ctx.thread_data[i].file_start_times = malloc(max_files * sizeof(double));
        ctx.thread_data[i].file_end_times = malloc(max_files * sizeof(double));
        ctx.thread_data[i].file_bytes_received = malloc(max_files * sizeof(size_t));
        
        if (!ctx.thread_data[i].file_start_times || !ctx.thread_data[i].file_end_times || !ctx.thread_data[i].file_bytes_received) {
            printf("Failed to allocate memory for thread %d timing arrays\n", i);
            return 1;
        }
        
        printf("Thread %d: Adaptive receiving enabled on port %d\n", i, port);
        
        if (pthread_create(&ctx.threads[i], NULL, adaptive_receiver_thread, &ctx.thread_data[i]) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
    
    printf("Receiver ready! Waiting for sender connections...\n");
    
    // Wait for all connections to be established
    int connected = 0;
    while (connected < num_streams && running) {
        connected = 0;
        for (int i = 0; i < num_streams; i++) {
            if (ctx.thread_data[i].client_fd > 0) {
                connected++;
            }
        }
        usleep(10000); // 10ms
    }
    
    if (!running) {
        printf("Shutdown requested before test started\n");
        cleanup_adaptive_context(&adaptive_ctx);  // Clean up before exit
        return 1;
    }
    
    printf("All %d connections established. Starting adaptive file transfers...\n", num_streams);
    ctx.overall_start_time = get_time();
    
    // Wait for test completion
    while (ctx.files_completed < max_files && running) {
        sleep(1);
    }
    
    ctx.test_active = 0;
    ctx.overall_end_time = get_time();
    
    printf("Test completed, waiting for threads to finish...\n");
    
    // Wait for all threads to finish
    for (int i = 0; i < num_streams; i++) {
        pthread_join(ctx.threads[i], NULL);
        close(ctx.thread_data[i].listen_fd);
        free(ctx.thread_data[i].file_start_times);
        free(ctx.thread_data[i].file_end_times);
        free(ctx.thread_data[i].file_bytes_received);
    }
    
    // Final report
    double total_duration = ctx.overall_end_time - ctx.overall_start_time;
    // double total_throughput_gbps = (ctx.total_received * 8.0) / (total_duration * 1e9);
    double avg_throughput_gbps = ctx.total_throughput / max_files;
    
    printf("\n=== FINAL RESULTS ===\n");
    printf("Files completed: %d/%d\n", ctx.files_completed, max_files);
    printf("Total duration: %.3f seconds\n", total_duration);
    printf("Total received: %.2f MB\n", ctx.total_received / (1024.0 * 1024.0));
    printf("Average throughput: %.2f Gbps (%.2f MB/s)\n", 
           avg_throughput_gbps, avg_throughput_gbps * 1000.0 / 8.0);
    printf("Streams: %d\n", num_streams);
    printf("Per-stream average: %.2f Gbps\n", avg_throughput_gbps / num_streams);
    
    // Print final adaptive performance summary
    printf("\nFinal Performance Summary:\n");
    for (int i = 0; i < num_streams; i++) {
        thread_performance_t *perf = &adaptive_ctx.thread_perf[i];
        printf("Thread %d: %.1f MB/s, %zu chunks, performance factor: %.2f\n", 
               i, perf->throughput_mbps, perf->chunks_completed, perf->performance_factor);
    }
    
    // Cleanup
    cleanup_adaptive_context(&adaptive_ctx);  // Clean up adaptive context
    pthread_mutex_destroy(&ctx.stats_mutex);
    pthread_barrier_destroy(&ctx.file_barrier);
    free(ctx.threads);
    free(ctx.thread_data);    
    close(ctx.command_socket);
    
    printf("Adaptive receiver completed successfully\n");
    return 0;
}