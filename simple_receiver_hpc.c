// Compile and Run
// gcc -O3 -Wall -pthread -o simple_receiver_hpc simple_receiver_hpc.c
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

#define DEFAULT_STREAMS 4
#define DEFAULT_PORT 5303
#define CHUNK_SIZE (16 * 1024 * 1024)  // 16MB chunks
#define MAGIC_HEADER 0xdeadbeefcafebabe

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
    size_t start_offset;
    size_t chunk_size;
    pthread_mutex_t *file_mutex;
    double throughput_mbps;
    double duration;
} thread_data_t;

static volatile int running = 1;
static pthread_barrier_t start_barrier;

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
    if (listen_fd < 0) {
        perror("socket");
        return -1;
    }
    
    // Enable SO_REUSEADDR
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Set large buffer sizes for 10GbE
    int buffer_size = 64 * 1024 * 1024;  // 64MB
    setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    
    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return -1;
    }
    
    if (listen(listen_fd, 1) < 0) {
        perror("listen");
        close(listen_fd);
        return -1;
    }
    
    printf("Thread listening on port %d\n", port);
    return listen_fd;
}

void* receiver_thread(void* arg) {
    thread_data_t *data = (thread_data_t*)arg;
    
    // Accept connection
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    
    data->client_fd = accept(data->listen_fd, (struct sockaddr*)&client_addr, &client_len);
    if (data->client_fd < 0) {
        perror("accept");
        return NULL;
    }
    
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    printf("Thread %d: Connection from %s:%d\n", 
           data->thread_id, client_ip, ntohs(client_addr.sin_port));
    
    // Enable TCP_NODELAY for low latency
    int flag = 1;
    setsockopt(data->client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    
    // Wait for all threads to be ready
    pthread_barrier_wait(&start_barrier);
    
    double start_time = get_time();
    
    printf("Thread %d: Ready to receive %.2f MB at offset %zu\n", 
           data->thread_id,
           data->chunk_size / (1024.0 * 1024.0),
           data->start_offset);
    
    char *buffer = data->file_data + data->start_offset;
    size_t remaining = data->chunk_size;
    size_t total_received = 0;
    
    while (remaining > 0 && running) {
        ssize_t received = recv(data->client_fd, buffer, remaining, 0);
        
        if (received < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            perror("recv");
            break;
        } else if (received == 0) {
            printf("Thread %d: Sender closed connection\n", data->thread_id);
            break;
        }
        
        buffer += received;
        remaining -= received;
        total_received += received;
    }
    
    double end_time = get_time();
    double duration = end_time - start_time;
    double throughput = (total_received / (1024.0 * 1024.0)) / duration;
    data->throughput_mbps = throughput;
    data->duration = duration;
    
    printf("Thread %d: Completed %.2f MB in %.2f seconds (%.2f MB/s)\n",
           data->thread_id, total_received / (1024.0 * 1024.0), duration, throughput);
    
    close(data->client_fd);
    close(data->listen_fd);
    return NULL;
}

int save_file(const char *filename, char *data, size_t size) {
    FILE *file = fopen(filename, "wb");
    if (!file) {
        perror("fopen");
        return -1;
    }
    
    if (fwrite(data, 1, size, file) != size) {
        perror("fwrite");
        fclose(file);
        return -1;
    }
    
    fclose(file);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 1) {
        printf("Simple High-Performance File Receiver (One-time Transfer)\n");
        printf("Usage: %s [port] [streams] [output_dir] [save_to_disk]\n", argv[0]);
        printf("Example: %s 5303 4 /tmp/received/ 1\n", argv[0]);
        printf("Parameters:\n");
        printf("  port: Base port number (default: 5303)\n");
        printf("  streams: Number of parallel streams (default: 4)\n");
        printf("  output_dir: Directory to save received files (default: ./)\n");
        printf("  save_to_disk: Save file to disk (1) or keep in memory only (0) (default: 1)\n");
        return 1;
    }
    
    int base_port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    int num_streams = (argc > 2) ? atoi(argv[2]) : DEFAULT_STREAMS;
    const char *output_dir = (argc > 3) ? argv[3] : "./";
    int save_to_disk = (argc > 4) ? atoi(argv[4]) : 1;
    
    printf("Simple High-Performance File Receiver\n");
    printf("=====================================\n");
    printf("Base port: %d\n", base_port);
    printf("Streams: %d\n", num_streams);
    printf("Output directory: %s\n", output_dir);
    printf("Save to disk: %s\n", save_to_disk ? "enabled" : "disabled");
    printf("Waiting for sender...\n\n");
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Create control listener
    int control_fd = create_listener(base_port);
    if (control_fd < 0) {
        return 1;
    }
    
    // Accept control connection and receive header
    struct sockaddr_in sender_addr;
    socklen_t sender_len = sizeof(sender_addr);
    
    printf("Waiting for control connection on port %d...\n", base_port);
    int control_client = accept(control_fd, (struct sockaddr*)&sender_addr, &sender_len);
    if (control_client < 0) {
        perror("accept control");
        close(control_fd);
        return 1;
    }
    
    char sender_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &sender_addr.sin_addr, sender_ip, INET_ADDRSTRLEN);
    printf("Control connection from %s:%d\n", sender_ip, ntohs(sender_addr.sin_port));
    
    // Receive file header
    file_header_t header;
    if (recv(control_client, &header, sizeof(header), 0) != sizeof(header)) {
        perror("recv header");
        close(control_client);
        close(control_fd);
        return 1;
    }
    
    if (header.magic != MAGIC_HEADER) {
        printf("Invalid magic header: 0x%lx\n", header.magic);
        close(control_client);
        close(control_fd);
        return 1;
    }
    
    printf("File: %s\n", header.filename);
    printf("Size: %.2f MB\n", header.file_size / (1024.0 * 1024.0));
    
    // Allocate memory for the entire file
    char *file_data = malloc(header.file_size);
    if (!file_data) {
        perror("malloc file_data");
        close(control_client);
        close(control_fd);
        return 1;
    }
    
    // Initialize barrier for thread synchronization
    if (pthread_barrier_init(&start_barrier, NULL, num_streams) != 0) {
        perror("pthread_barrier_init");
        free(file_data);
        close(control_client);
        close(control_fd);
        return 1;
    }
    
    // Calculate chunk sizes for each stream
    size_t chunk_size = header.file_size / num_streams;
    size_t remainder = header.file_size % num_streams;
    
    pthread_t threads[num_streams];
    thread_data_t thread_data[num_streams];
    pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;
    
    // Create data stream listeners and threads
    for (int i = 0; i < num_streams; i++) {
        int port = base_port + 1 + i;  // Data ports start at base_port + 1
        int listen_fd = create_listener(port);
        if (listen_fd < 0) {
            return 1;
        }
        
        thread_data[i].thread_id = i;
        thread_data[i].listen_fd = listen_fd;
        thread_data[i].client_fd = -1;
        thread_data[i].port = port;
        thread_data[i].file_data = file_data;
        thread_data[i].start_offset = i * chunk_size;
        thread_data[i].chunk_size = chunk_size + (i == num_streams - 1 ? remainder : 0);
        thread_data[i].file_mutex = &file_mutex;
        thread_data[i].throughput_mbps = 0.0;
        thread_data[i].duration = 0.0;
        
        if (pthread_create(&threads[i], NULL, receiver_thread, &thread_data[i]) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
    
    printf("Data stream listeners created. Sending ready signal...\n");
    
    // Send ready signal to sender
    char ready_signal = 'R';
    if (send(control_client, &ready_signal, 1, 0) != 1) {
        perror("send ready signal");
        close(control_client);
        close(control_fd);
        free(file_data);
        return 1;
    }
    
    close(control_client);
    close(control_fd);
    
    // Wait for all receiver threads to complete
    for (int i = 0; i < num_streams; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Find maximum thread duration and sum throughputs
    double max_duration = 0.0;
    double total_throughput = 0.0;
    for (int i = 0; i < num_streams; i++) {
        total_throughput += thread_data[i].throughput_mbps;
        if (thread_data[i].duration > max_duration) {
            max_duration = thread_data[i].duration;
        }
    }
    double total_throughput_gbps = total_throughput * 8 / 1000.0;  // Convert MB/s to Gbps (MB/s * 8 / 1000)
    
    printf("\n=== Transfer Complete ===\n");
    printf("Total size: %.2f MB\n", header.file_size / (1024.0 * 1024.0));
    printf("Duration: %.2f seconds\n", max_duration);
    printf("Total throughput: %.2f MB/s (%.2f Gbps)\n", total_throughput, total_throughput_gbps);
    printf("Streams used: %d\n", num_streams);
    
    // Save file to disk if requested
    if (save_to_disk) {
        char output_path[512];
        snprintf(output_path, sizeof(output_path), "%s/%s", output_dir, header.filename);
        
        printf("Saving file to: %s\n", output_path);
        if (save_file(output_path, file_data, header.file_size) == 0) {
            printf("File saved successfully!\n");
        } else {
            printf("Failed to save file\n");
        }
    } else {
        printf("File kept in memory only (not saved to disk)\n");
    }
    
    free(file_data);
    pthread_barrier_destroy(&start_barrier);
    pthread_mutex_destroy(&file_mutex);
    
    return 0;
}

