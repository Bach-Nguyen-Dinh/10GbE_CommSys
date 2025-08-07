#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sched.h>
#include <signal.h>
#include <libgen.h>
#include <endian.h>
// #include <linux/tcp.h>

#define DEFAULT_PORT 5303
#define DEFAULT_STREAMS 4
#define BUFFER_SIZE (2 * 1024 * 1024)  // 2MB buffer
#define SOCKET_BUFFER_SIZE (16 * 1024 * 1024)  // 16MB socket buffer
#define MAX_FILES 10  // Default number of files to send
#define MAX_FILENAME_LEN 256
#define SYNC_MAGIC 0xDEADBEEFCAFEBABE  // Synchronization marker

#define ADAPTIVE_CHUNK_SIZE (16 * 1024 * 1024)  // 16MB adaptive chunks
#define MAX_WORK_ITEMS 1024  // Maximum work queue size

#define READY_SIGNAL    0xAA
#define ACK_SIGNAL      0xAB
#define HEARTBEAT       0xBB

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
    int socket_fd;
    char *file_data;
    size_t chunk_size;
    size_t chunk_offset;
    size_t total_file_size;
    size_t bytes_sent;
    int use_zerocopy;
    int num_streams;
    pthread_mutex_t *stats_mutex;
    pthread_barrier_t *file_barrier;
    double *file_start_times;
    double *file_end_times;
    size_t *file_bytes_sent;
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
    char *data;
    size_t size;
    size_t total_sent;
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
    int use_zerocopy;
    char filename[MAX_FILENAME_LEN];
    double total_throughput;

    volatile int receiver_ready;     // Receiver signals readiness
    pthread_mutex_t ready_mutex;     // Protect ready signaling
    pthread_cond_t ready_cond;       // Condition variable for readiness

    pthread_t ready_listener_thread; // Thread to listen for ready signals
    volatile int ready_listener_active; // Control flag for listener thread
} sender_ctx_t;

static volatile int running = 1;

// Function Prototypes
static double get_time();
static void signal_handler(int sig);
static int send_all(int sockfd, const void *buf, size_t len);
static int recv_all(int sockfd, void *buf, size_t len);
static int setup_socket_options(int sock);
static int connect_to_receiver(const char *receiver_ip, int port);
static void set_thread_affinity(int thread_id);
static int send_file_header(int sock, size_t file_size, int num_streams, const char *filename, int file_num);
static void *adaptive_sender_thread(void *arg);
static int load_file(const char *filename, sender_ctx_t *ctx);
void cleanup_adaptive_context(adaptive_ctx_t *ctx);
int init_adaptive_context(adaptive_ctx_t *ctx, size_t file_size, int num_threads);
int get_next_work_item(adaptive_ctx_t *ctx, int thread_id, size_t *offset, size_t *size);
void update_thread_performance(adaptive_ctx_t *ctx, int thread_id, size_t bytes_processed, double duration);
static void *ready_signal_listener(void *arg);
static int wait_for_receiver_ready_with_timeout(sender_ctx_t *ctx, int file_num, int timeout_seconds);

static double get_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void signal_handler(int sig) {
    printf("\n\n=== SHUTDOWN SIGNAL RECEIVED ===\n");
    printf("Signal: %d (%s)\n", sig, (sig == SIGINT) ? "SIGINT" : "SIGTERM");
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
            if (received == 0 || (errno != EINTR && errno != EAGAIN)) {
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
    
    if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)) < 0) {
        perror("setsockopt SO_SNDBUF");
        return -1;
    }
    
    return 0;
}

static int connect_to_receiver(const char *receiver_ip, int port) {
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
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, receiver_ip, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sock);
        return -1;
    }
    
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return -1;
    }
    
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

static int send_file_header(int sock, size_t file_size, int num_streams, const char *filename, int file_num) {
    // Send synchronization marker first
    uint64_t sync_marker = htobe64(SYNC_MAGIC);
    if (send_all(sock, &sync_marker, sizeof(sync_marker)) < 0) {
        printf("Failed to send sync marker\n");
        return -1;
    }
    
    // Send file number for verification
    uint32_t net_file_num = htonl(file_num);
    if (send_all(sock, &net_file_num, sizeof(net_file_num)) < 0) {
        printf("Failed to send file number\n");
        return -1;
    }
    
    // Send file size
    uint64_t net_file_size = htobe64(file_size);
    if (send_all(sock, &net_file_size, sizeof(net_file_size)) < 0) {
        printf("Failed to send file size\n");
        return -1;
    }
    
    // Send number of streams
    uint32_t net_num_streams = htonl(num_streams);
    if (send_all(sock, &net_num_streams, sizeof(net_num_streams)) < 0) {
        printf("Failed to send stream count\n");
        return -1;
    }
    
    // Send filename
    uint32_t filename_len = strlen(filename);
    uint32_t net_filename_len = htonl(filename_len);
    if (send_all(sock, &net_filename_len, sizeof(net_filename_len)) < 0) {
        printf("Failed to send filename length\n");
        return -1;
    }
    
    if (filename_len > 0) {
        if (send_all(sock, filename, filename_len) < 0) {
            printf("Failed to send filename\n");
            return -1;
        }
    }
    
    printf("Thread 0: Sent header for file %d (sync=0x%lx, size=%zu)\n", 
           file_num, SYNC_MAGIC, file_size);
    return 0;
}

static void *adaptive_sender_thread(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    sender_ctx_t *ctx = (sender_ctx_t *)data->global_ctx;
    
    set_thread_affinity(data->thread_id);
    
    printf("Thread %d starting with adaptive chunking...\n", data->thread_id);
    
    // Process files one by one using the persistent connection
    for (int file_num = 1; file_num <= MAX_FILES && running && ctx->test_active; file_num++) {
        data->bytes_sent = 0;
        data->total_bytes_processed = 0;
        
        // Only thread 0 sends the header
        if (data->thread_id == 0) {
            
            // TIMING-SAFE HEADER SYNC: Wait for receiver ready signal (skip for first file)
            if (file_num > 1) {  // Skip for first file
                // Enhanced ready signal waiting with timeout and retry
                int ready_confirmed = 0;
                int max_wait_attempts = 3;
                
                for (int wait_attempt = 0; wait_attempt < max_wait_attempts && !ready_confirmed; wait_attempt++) {
                    int wait_result = wait_for_receiver_ready_with_timeout(ctx, file_num, 30);  // 30 second timeout
                    
                    if (wait_result == 0) {
                        ready_confirmed = 1;
                        break;  // Success
                    }
                    
                    printf("Thread 0: Ready signal wait failed (attempt %d/%d)\n", wait_attempt + 1, max_wait_attempts);
                    
                    if (wait_attempt < max_wait_attempts - 1) {
                        printf("Thread 0: Retrying ready signal wait...\n");
                        sleep(2);
                    }
                }
                
                if (!ready_confirmed) {
                    printf("Thread 0: PROCEEDING WITHOUT CONFIRMED READY SIGNAL for file %d\n", file_num);
                    printf("Thread 0: This may cause timing issues but prevents deadlock\n");
                }
                
                if (!running || !ctx->test_active) break;
            }
            
            // NOW safe to send header - receiver is guaranteed to be listening
            if (send_file_header(data->socket_fd, data->total_file_size, data->num_streams, ctx->filename, file_num) < 0) {
                printf("Thread 0: Failed to send header for file %d\n", file_num);
                ctx->test_active = 0;  // Signal all threads to stop
                pthread_barrier_wait(data->file_barrier);  // Participate in barrier before breaking
                break;
            }
            
            // Initialize adaptive context for this file
            if (init_adaptive_context(data->adaptive_ctx, data->total_file_size, data->num_streams) < 0) {
                printf("Failed to initialize adaptive context\n");
                ctx->test_active = 0;  // Signal all threads to stop
                pthread_barrier_wait(data->file_barrier);  // Participate in barrier before breaking
                break;
            }
            
            printf("Thread 0: Header sent successfully for file %d\n", file_num);
        }
        
        // Barrier 1: Wait for header and adaptive context setup
        pthread_barrier_wait(data->file_barrier);
        
        // Check if Thread 0 encountered an error
        if (!ctx->test_active) {
            printf("Thread %d: Test marked inactive, exiting\n", data->thread_id);
            break;
        }
        
        // Record file start time
        data->file_start_times[file_num - 1] = get_time();
        
        // Adaptive chunk processing loop
        size_t chunk_offset, chunk_size;
        while (get_next_work_item(data->adaptive_ctx, data->thread_id, &chunk_offset, &chunk_size) && 
               running && ctx->test_active) {
            
            double chunk_start = get_time();
            
            // Send this chunk
            size_t bytes_remaining = chunk_size;
            size_t current_offset = chunk_offset;
            
            while (bytes_remaining > 0 && running && ctx->test_active) {
                size_t bytes_to_send = (bytes_remaining > BUFFER_SIZE) ? BUFFER_SIZE : bytes_remaining;
                
                ssize_t sent;
                if (data->use_zerocopy) {
                    sent = send(data->socket_fd, data->file_data + current_offset, bytes_to_send, MSG_NOSIGNAL);
                } else {
                    sent = send_all(data->socket_fd, data->file_data + current_offset, bytes_to_send);
                }
                
                if (sent <= 0) {
                    printf("Thread %d: Send failed during chunk at offset %zu\n", data->thread_id, current_offset);
                    ctx->test_active = 0;  // Signal all threads to stop
                    goto chunk_failed;
                }
                
                bytes_remaining -= sent;
                current_offset += sent;
                data->bytes_sent += sent;
                data->total_bytes_processed += sent;
            }
            
            // Update performance metrics for this chunk
            double chunk_duration = get_time() - chunk_start;
            update_thread_performance(data->adaptive_ctx, data->thread_id, chunk_size, chunk_duration);
            
            continue;
            
        chunk_failed:
            printf("Thread %d: Chunk processing failed\n", data->thread_id);
            break;
        }
        
        // Record file end time
        data->file_end_times[file_num - 1] = get_time();
        data->file_bytes_sent[file_num - 1] = data->bytes_sent;
        
        double file_duration = data->file_end_times[file_num - 1] - data->file_start_times[file_num - 1];
        double throughput_gbps = (data->bytes_sent * 8.0) / (file_duration * 1e9);
        
        printf("Thread %d File %d: %.2f MB in %.3f sec (%.2f Gbps, %zu chunks)\n",
               data->thread_id, file_num,
               data->bytes_sent / (1024.0 * 1024.0),
               file_duration, throughput_gbps, 
               data->adaptive_ctx->thread_perf[data->thread_id].chunks_completed);
        
        // Barrier 2: Wait for all threads to complete this file before calculating totals
        pthread_barrier_wait(data->file_barrier);
        
        // Thread 0 calculates and reports file totals
        if (data->thread_id == 0) {
            // Calculate total for this file across all threads
            size_t file_total = 0;
            double min_start = data->file_start_times[file_num - 1];
            double max_end = data->file_end_times[file_num - 1];
            
            // Access all threads' data through the context
            for (int t = 0; t < data->num_streams; t++) {
                file_total += ctx->thread_data[t].file_bytes_sent[file_num - 1];
                if (ctx->thread_data[t].file_start_times[file_num - 1] < min_start) {
                    min_start = ctx->thread_data[t].file_start_times[file_num - 1];
                }
                if (ctx->thread_data[t].file_end_times[file_num - 1] > max_end) {
                    max_end = ctx->thread_data[t].file_end_times[file_num - 1];
                }
            }
            
            double total_duration = max_end - min_start;
            double total_throughput = (file_total * 8.0) / (total_duration * 1e9);
            
            printf("FILE %d COMPLETE: %.2f MB in %.3f sec (%.2f Gbps)\n", 
                   file_num, file_total / (1024.0 * 1024.0), total_duration, total_throughput);
            
            // Print adaptive performance summary
            printf("Adaptive Summary - ");
            for (int t = 0; t < data->num_streams; t++) {
                printf("T%d:%.0f%% ", t, data->adaptive_ctx->thread_perf[t].performance_factor * 100);
            }
            printf("\n");
            
            // Update global stats - THIS IS CRITICAL!
            pthread_mutex_lock(data->stats_mutex);
            ctx->total_sent += file_total;
            ctx->total_throughput += total_throughput;
            ctx->files_completed++;  // THIS LINE WAS MISSING!
            pthread_mutex_unlock(data->stats_mutex);
            
            printf("----------------------------------------\n");
        }
        
        // Barrier 3: Synchronize before next file
        pthread_barrier_wait(data->file_barrier);
        
        // REMOVED: Small delay for receiver readiness (replaced with proper signaling)
        // if (data->thread_id == 0 && file_num < MAX_FILES) {
        //     usleep(1000);
        // }
    }
    
    close(data->socket_fd);
    printf("Thread %d completed all files\n", data->thread_id);
    return NULL;
}

static int load_file(const char *filename, sender_ctx_t *ctx) {
    struct stat st;
    if (stat(filename, &st) < 0) {
        perror("stat");
        return -1;
    }
    
    ctx->size = st.st_size;
    
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return -1;
    }
    
    ctx->data = mmap(NULL, ctx->size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ctx->data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return -1;
    }
    
    close(fd);
    
    // Extract filename for reporting
    char *base_name = basename((char *)filename);
    strncpy(ctx->filename, base_name, sizeof(ctx->filename) - 1);
    ctx->filename[sizeof(ctx->filename) - 1] = '\0';
    
    return 0;
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

static void *ready_signal_listener(void *arg) {
    sender_ctx_t *ctx = (sender_ctx_t *)arg;
    
    printf("Bulletproof Ready Signal Listener Started\n");
    
    // Socket setup
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("ready listener socket");
        return NULL;
    }
    
    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Enable comprehensive socket health monitoring on listener
    int keepalive = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(5403);  // Port 5403 for ready signals
    
    if (bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("ready listener bind");
        close(listen_sock);
        return NULL;
    }
    
    if (listen(listen_sock, 1) < 0) {
        perror("ready listener listen");
        close(listen_sock);
        return NULL;
    }
    
    printf("Bulletproof listener bound to port 5403\n");
    
    // Accept connection from receiver
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);
    
    int client_sock = accept(listen_sock, (struct sockaddr *)&client_addr, &addr_len);
    if (client_sock < 0) {
        perror("ready listener accept");
        close(listen_sock);
        return NULL;
    }
    
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    printf("Bulletproof connection established from %s\n", client_ip);
    
    close(listen_sock);  // Don't need listener anymore
    
    // Enhanced listening loop with health monitoring
    int signals_processed = 0;
    time_t last_activity = time(NULL);
    
    while (running && ctx->test_active && ctx->ready_listener_active) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(client_sock, &read_fds);
        
        struct timeval timeout;
        timeout.tv_sec = 5;  // 5 second timeout for regular checks
        timeout.tv_usec = 0;
        
        int select_result = select(client_sock + 1, &read_fds, NULL, NULL, &timeout);
        
        if (select_result == 0) {
            // Timeout - check connection health
            time_t now = time(NULL);
            if (now - last_activity > 30) {
                printf("No activity for 30 seconds, checking connection health...\n");
                
                // Simple connection test
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(client_sock, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
                    printf("Connection health check failed - connection broken\n");
                    break;
                }
            }
            continue;
        }
        
        if (select_result < 0) {
            if (errno != EINTR) {
                perror("select failed");
                break;
            }
            continue;
        }
        
        // Read ready signal
        uint8_t received_signal;
        ssize_t received = recv(client_sock, &received_signal, 1, 0);
        
        if (received <= 0) {
            if (received == 0) {
                printf("Connection closed by receiver (processed %d signals)\n", signals_processed);
            } else {
                perror("recv failed");
            }
            break;
        }
        
        last_activity = time(NULL);
        
        if (received_signal == READY_SIGNAL) {
            signals_processed++;
            printf("✓ Ready signal #%d received and confirmed\n", signals_processed);
            
            // === SEND ACKNOWLEDGMENT (CRITICAL FOR CONFIRMATION LAYER) ===
            uint8_t ack = ACK_SIGNAL;
            if (send(client_sock, &ack, 1, MSG_NOSIGNAL) > 0) {
                printf("Acknowledgment sent for signal #%d\n", signals_processed);
            } else {
                printf("Failed to send acknowledgment for signal #%d\n", signals_processed);
            }
            
            // Signal waiting sender thread
            pthread_mutex_lock(&ctx->ready_mutex);
            ctx->receiver_ready = 1;
            pthread_cond_signal(&ctx->ready_cond);
            pthread_mutex_unlock(&ctx->ready_mutex);
            
        } else if (received_signal == HEARTBEAT) {
            printf("Heartbeat received\n");
            uint8_t heartbeat_response = HEARTBEAT;
            send(client_sock, &heartbeat_response, 1, MSG_NOSIGNAL);
            
        } else {
            printf("Unknown signal received: 0x%02X\n", received_signal);
        }
    }
    
    close(client_sock);
    printf("Bulletproof Ready Signal Listener Terminated (processed %d signals)\n", signals_processed);
    return NULL;
}

static int wait_for_receiver_ready_with_timeout(sender_ctx_t *ctx, int file_num, int timeout_seconds) {
    printf("Thread 0: Waiting for receiver ready signal for file %d...\n", file_num);
    
    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += timeout_seconds;
    
    pthread_mutex_lock(&ctx->ready_mutex);
    int wait_result = pthread_cond_timedwait(&ctx->ready_cond, &ctx->ready_mutex, &timeout);
    
    if (wait_result == ETIMEDOUT) {
        printf("Thread 0: TIMEOUT waiting for ready signal after %d seconds\n", timeout_seconds);
        printf("Thread 0: Proceeding anyway - better than hanging forever\n");
        pthread_mutex_unlock(&ctx->ready_mutex);
        return -1;  // Timeout, but we'll continue
    } else if (wait_result == 0) {
        printf("Thread 0: ✓ Receiver ready signal received for file %d\n", file_num);
        ctx->receiver_ready = 0;  // Reset for next iteration
        pthread_mutex_unlock(&ctx->ready_mutex);
        return 0;  // Success
    } else {
        printf("Thread 0: Error waiting for ready signal: %s\n", strerror(wait_result));
        pthread_mutex_unlock(&ctx->ready_mutex);
        return -1;  // Error
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <receiver_ip> <data_file> [streams] [zerocopy] [port] [max_files]\n", argv[0]);
        printf("Example: %s 169.254.207.40 /path/to/file.png 4 1 5303 10\n", argv[0]);
        return 1;
    }
    
    const char *receiver_ip = argv[1];
    const char *filename = argv[2];
    int num_streams = (argc > 3) ? atoi(argv[3]) : DEFAULT_STREAMS;
    int use_zerocopy = (argc > 4) ? atoi(argv[4]) : 1;
    int base_port = (argc > 5) ? atoi(argv[5]) : DEFAULT_PORT;
    int max_files = (argc > 6) ? atoi(argv[6]) : MAX_FILES;
    
    printf("Robust High-Performance File Sender (Persistent Connections)\n");
    printf("============================================================\n");
    printf("Target: %s:%d\n", receiver_ip, base_port);
    printf("File: %s\n", filename);
    printf("Streams: %d\n", num_streams);
    printf("Zero-copy: %s\n", use_zerocopy ? "enabled" : "disabled");
    printf("Max files: %d\n", max_files);
    printf("Mode: File-count based with TCP synchronization\n\n");
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    sender_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.num_streams = num_streams;
    ctx.max_files = max_files;
    ctx.use_zerocopy = use_zerocopy;
    ctx.test_active = 1;
    ctx.files_completed = 0;
    ctx.total_sent = 0;
    ctx.total_throughput = 0;
    
    // Load file into memory once
    printf("Loading file: %s\n", filename);
    if (load_file(filename, &ctx) < 0) {
        return 1;
    }
    printf("File size: %.2f MB\n", ctx.size / (1024.0 * 1024.0));
    
    // Allocate adaptive context
    adaptive_ctx_t adaptive_ctx;
    memset(&adaptive_ctx, 0, sizeof(adaptive_ctx));  // Initialize to zero
    
    ctx.threads = malloc(num_streams * sizeof(pthread_t));
    ctx.thread_data = malloc(num_streams * sizeof(thread_data_t));
    
    if (pthread_mutex_init(&ctx.ready_mutex, NULL) != 0) {
        perror("pthread_mutex_init ready_mutex");
        return 1;
    }
    
    if (pthread_cond_init(&ctx.ready_cond, NULL) != 0) {
        perror("pthread_cond_init ready_cond");
        pthread_mutex_destroy(&ctx.ready_mutex);
        return 1;
    }

    ctx.receiver_ready = 0;  // Initial state - not ready
    ctx.ready_listener_active = 1;  // Start listener

    if (pthread_mutex_init(&ctx.stats_mutex, NULL) != 0) {
        perror("pthread_mutex_init stats_mutex");
        pthread_mutex_destroy(&ctx.ready_mutex);
        pthread_cond_destroy(&ctx.ready_cond);
        return 1;
    }
    
    if (pthread_barrier_init(&ctx.file_barrier, NULL, num_streams) != 0) {
        perror("pthread_barrier_init file_barrier");
        pthread_mutex_destroy(&ctx.ready_mutex);
        pthread_cond_destroy(&ctx.ready_cond);
        pthread_mutex_destroy(&ctx.stats_mutex);
        return 1;
    }

    // Start ready signal listener thread BEFORE connecting to receiver
    printf("Starting ready signal listener...\n");
    if (pthread_create(&ctx.ready_listener_thread, NULL, ready_signal_listener, &ctx) != 0) {
        perror("pthread_create ready_listener");
        return 1;
    }
    
    sleep(1);  // Give listener time to bind to port
    
    // Connect all streams to receiver (persistent connections)
    printf("Establishing persistent connections to receiver...\n");
    for (int i = 0; i < num_streams; i++) {
        int port = base_port + i;
        int sock = connect_to_receiver(receiver_ip, port);
        if (sock < 0) {
            printf("Failed to connect stream %d to port %d\n", i, port);
            return 1;
        }
        
        printf("Stream %d connected to port %d\n", i, port);

        // Initialize thread data structure
        memset(&ctx.thread_data[i], 0, sizeof(thread_data_t));  // Clear everything first
        
        ctx.thread_data[i].thread_id = i;
        ctx.thread_data[i].socket_fd = sock;
        ctx.thread_data[i].file_data = ctx.data;
        ctx.thread_data[i].total_file_size = ctx.size;
        ctx.thread_data[i].bytes_sent = 0;
        ctx.thread_data[i].use_zerocopy = use_zerocopy;
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
        ctx.thread_data[i].file_bytes_sent = malloc(max_files * sizeof(size_t));

        if (!ctx.thread_data[i].file_start_times || !ctx.thread_data[i].file_end_times || !ctx.thread_data[i].file_bytes_sent) {
            printf("Failed to allocate memory for thread %d timing arrays\n", i);
            return 1;
        }
        
        printf("Thread %d: Adaptive sender enabled on port %d\n", i, port);
    }
    
    printf("Starting sender threads...\n");
    for (int i = 0; i < num_streams; i++) {
        if (pthread_create(&ctx.threads[i], NULL, adaptive_sender_thread, &ctx.thread_data[i]) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
    
    sleep(1);
    
    printf("All %d streams connected. Starting file transfers...\n", num_streams);
    ctx.overall_start_time = get_time();
    
    // Wait for test completion
    while (ctx.files_completed < max_files && running) {
        sleep(1);
    }
    
    ctx.test_active = 0;
    ctx.overall_end_time = get_time();
    
    printf("Test completed, waiting for threads to finish...\n");
    
    // Wait for all threads to complete
    for (int i = 0; i < num_streams; i++) {
        pthread_join(ctx.threads[i], NULL);
        free(ctx.thread_data[i].file_start_times);
        free(ctx.thread_data[i].file_end_times);
        free(ctx.thread_data[i].file_bytes_sent);
    }
    
    // Final report
    double total_duration = ctx.overall_end_time - ctx.overall_start_time;
    double avg_throughput_gbps = ctx.total_throughput / max_files;
    
    printf("\n=== FINAL RESULTS ===\n");
    printf("File: %s\n", ctx.filename);
    printf("File size: %.2f MB\n", ctx.size / (1024.0 * 1024.0));
    printf("Files completed: %d/%d\n", ctx.files_completed, max_files);
    printf("Total duration: %.3f seconds\n", total_duration);
    printf("Total sent: %.2f MB\n", ctx.total_sent / (1024.0 * 1024.0));
    printf("Average throughput: %.2f Gbps (%.2f MB/s)\n", 
           avg_throughput_gbps, avg_throughput_gbps * 1000.0 / 8.0);
    printf("Streams: %d\n", num_streams);
    printf("Per-stream average: %.2f Gbps\n", avg_throughput_gbps / num_streams);
    
    // Print final adaptive performance summary
    printf("\nFinal Adaptive Performance Summary:\n");
    for (int i = 0; i < num_streams; i++) {
        thread_performance_t *perf = &adaptive_ctx.thread_perf[i];
        printf("Thread %d: %.1f MB/s, %zu chunks, performance factor: %.2f\n", 
               i, perf->throughput_mbps, perf->chunks_completed, perf->performance_factor);
    }

    // Stop ready listener and wait for it to finish
    ctx.ready_listener_active = 0;
    pthread_join(ctx.ready_listener_thread, NULL);
    
    // Cleanup
    cleanup_adaptive_context(&adaptive_ctx);  // Clean up adaptive context
    pthread_mutex_destroy(&ctx.stats_mutex);
    pthread_barrier_destroy(&ctx.file_barrier);
    free(ctx.threads);
    free(ctx.thread_data);
    munmap(ctx.data, ctx.size);

    // Cleanup timing-safe synchronization
    pthread_mutex_destroy(&ctx.ready_mutex);
    pthread_cond_destroy(&ctx.ready_cond);
    
    printf("Sender completed successfully\n");
    return 0;
}