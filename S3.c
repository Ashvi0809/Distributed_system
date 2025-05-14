#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <libgen.h>
#include <signal.h>
#include <limits.h>
#include <errno.h>

#define BUFFER_SIZE 1024

// Global flag to control server shutdown
static volatile sig_atomic_t keep_running = 1;
// Server socket descriptor
static int server_sock = -1;

// Signal handler for graceful shutdown
void signal_handler(int sig) {
    keep_running = 0;
    if (server_sock != -1) close(server_sock);
}

// Function to create directories recursively
void create_directories(const char *path);

int main(int argc, char *argv[]) {
    // Validate command-line arguments
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <S3_port>\n", argv[0]);
        return 1;
    }

    // Parse and validate port number
    int PORT_S3 = atoi(argv[1]);
    if (PORT_S3 < 1024 || PORT_S3 > 65535) {
        fprintf(stderr, "Error: Port must be between 1024 and 65535\n");
        return 1;
    }

    // Initialize server and client address structures
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_len = sizeof(client_addr);
    // Set up signal handler for SIGINT
    struct sigaction sa = {.sa_handler = signal_handler, .sa_flags = SA_RESTART};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);

    // Create server socket
    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    // Allow socket reuse
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT_S3);

    // Bind socket to address
    if (bind(server_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_sock);
        return 1;
    }
    // Listen for incoming connections
    listen(server_sock, 5);
    printf("S3 listening on port %d...\n", PORT_S3);

    // Main server loop
    while (keep_running) {
        // Accept client connection
        int client_sock = accept(server_sock, (struct sockaddr*)&client_addr, &addr_len);
        if (client_sock < 0) {
            if (!keep_running) break;
            continue;
        }

        // Receive command from client
        char buffer[BUFFER_SIZE];
        ssize_t received = recv(client_sock, buffer, BUFFER_SIZE - 1, 0);
        if (received <= 0) {
            close(client_sock);
            continue;
        }
        buffer[received] = '\0';

        // Parse command and parameters
        char command[20], param1[PATH_MAX] = {0};
        sscanf(buffer, "%s %[^\n]", command, param1);

        if (strcmp(command, "uploadf") == 0) {
            printf("S3: Received uploadf command: %s\n", buffer);
            char filename[256], dest_path[PATH_MAX];
            sscanf(buffer, "%*s %s %s", filename, dest_path);
            // Construct full file path
            char full_path[PATH_MAX];
            if (dest_path[strlen(dest_path) - 1] == '/')
                snprintf(full_path, PATH_MAX, "%s%s", dest_path, filename);
            else
                snprintf(full_path, PATH_MAX, "%s/%s", dest_path, filename);
            printf("S3: Attempting to write to %s\n", full_path);

            // Create necessary directories
            char *dir_path = strdup(full_path);
            create_directories(dirname(dir_path));
            free(dir_path);

            // Remove existing file, if any
            remove(full_path);
            // Open file for writing
            FILE *fp = fopen(full_path, "wb");
            if (!fp) {
                char error_msg[BUFFER_SIZE];
                snprintf(error_msg, BUFFER_SIZE, "Upload failed: Cannot write file (%s)", strerror(errno));
                send(client_sock, error_msg, strlen(error_msg), 0);
                close(client_sock);
                continue;
            }

            // Receive and write file data
            size_t total_bytes = 0;
            ssize_t bytes;
            while ((bytes = recv(client_sock, buffer, BUFFER_SIZE, 0)) > 0) {
                if (fwrite(buffer, 1, bytes, fp) != bytes) {
                    fclose(fp);
                    remove(full_path);
                    send(client_sock, "Upload failed: Error writing file", 33, 0);
                    close(client_sock);
                    continue;
                }
                total_bytes += bytes;
                printf("S3: Received %zd bytes, total %zu\n", bytes, total_bytes);
            }
            fclose(fp);

            // Check for receive errors
            if (bytes < 0) {
                remove(full_path);
                send(client_sock, "Upload failed: Error receiving data", 36, 0);
                close(client_sock);
                continue;
            }

            // Send response based on success
            if (total_bytes > 0) {
                send(client_sock, "Stored successfully", 20, 0);
                printf("S3: Stored %s (%zu bytes)\n", full_path, total_bytes);
            } else {
                remove(full_path);
                send(client_sock, "Upload failed: No data received", 32, 0);
            }
        } else if (strcmp(command, "downlf") == 0) {
            printf("S3: Received downlf command: %s\n", buffer);
            // Open requested file
            FILE *fp = fopen(param1, "rb");
            if (!fp) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Download failed: File not found", 32, 0);
                close(client_sock);
                continue;
            }

            // Get file size
            fseek(fp, 0, SEEK_END);
            uint64_t file_size = ftell(fp);
            rewind(fp);
            uint64_t net_size = htobe64(file_size);
            // Send file size to client
            send(client_sock, (char*)&net_size, sizeof(net_size), 0);
            printf("S3: Sending file %s (%lu bytes)\n", param1, file_size);

            // Send file data
            size_t bytes;
            while ((bytes = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
                send(client_sock, buffer, bytes, 0);
            }
            fclose(fp);
            // Signal end of data
            shutdown(client_sock, SHUT_WR);
            printf("S3: File transfer complete for %s\n", param1);
        } else if (strcmp(command, "removef") == 0) {
            printf("S3: Received removef command: %s\n", buffer);
            char filepath[PATH_MAX];
            sscanf(buffer, "%*s %s", filepath);

            // Check if file exists
            struct stat statbuf;
            if (stat(filepath, &statbuf) == 0) {
                if (S_ISREG(statbuf.st_mode)) {
                    // Attempt to remove file
                    if (remove(filepath) == 0) {
                        send(client_sock, "File removed successfully", 25, 0);
                        printf("S3: Removed %s\n", filepath);
                    } else {
                        send(client_sock, "Remove failed: Permission denied", 32, 0);
                    }
                } else {
                    send(client_sock, "Remove failed: Not a regular file", 33, 0);
                }
            } else {
                send(client_sock, "Remove failed: File not found", 29, 0);
            }
        } else if (strcmp(command, "downltar") == 0) {
            printf("S3: Received downltar command: %s\n", buffer);
            char filetype[16];
            sscanf(buffer, "%*s %s", filetype);
            // Validate file type
            if (strcmp(filetype, ".txt") != 0) {
                send(client_sock, "Download failed: Invalid file type for this server", 50, 0);
                close(client_sock);
                continue;
            }

            // Construct tar file path
            char tar_path[PATH_MAX];
            char *home = getenv("HOME");
            snprintf(tar_path, PATH_MAX, "%s/S3/temp/textfiles.tar", home);
            create_directories(dirname(strdup(tar_path)));

            // Create tar of .txt files
            char cmd[BUFFER_SIZE];
            snprintf(cmd, BUFFER_SIZE, "cd %s/S3 && find * -type f -name '*.txt' | tar -cf %s -T -", home, tar_path);
            int ret = system(cmd);
            if (ret != 0) {
                send(client_sock, "Download failed: No files found or tar creation failed", 54, 0);
                remove(tar_path);
                close(client_sock);
                continue;
            }

            // Verify tar file
            struct stat statbuf;
            if (stat(tar_path, &statbuf) != 0) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Tar file not found", 18, 0);
                remove(tar_path);
                close(client_sock);
                continue;
            }
            
            // Send tar file size
            uint64_t file_size = statbuf.st_size;
            uint64_t net_size = htobe64(file_size);
            send(client_sock, (char*)&net_size, sizeof(net_size), 0);
            
            // Open and send tar file
            FILE *fp = fopen(tar_path, "rb");
            if (!fp) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Cannot open tar file", 21, 0);
                remove(tar_path);
                close(client_sock);
                continue;
            }
            
            size_t bytes;
            while ((bytes = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
                send(client_sock, buffer, bytes, 0);
            }
            fclose(fp);
            // Clean up tar file
            remove(tar_path);
            printf("S3: Sent %s to S1\n", tar_path);
        } else if (strcmp(command, "dispfnames") == 0) {
            printf("S3: Received dispfnames command: %s\n", buffer);
            char pathname[PATH_MAX], filetype[16];
            sscanf(buffer, "%*s %s %s", pathname, filetype);
            // Validate file type
            if (strcmp(filetype, ".txt") != 0) {
                send(client_sock, "No files found", 14, 0);
                close(client_sock);
                continue;
            }

            // Verify directory exists
            struct stat statbuf;
            if (stat(pathname, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode)) {
                send(client_sock, "No files found", 14, 0);
                close(client_sock);
                continue;
            }

            // Collect file names
            char file_list[BUFFER_SIZE] = {0};
            char cmd[BUFFER_SIZE];
            snprintf(cmd, BUFFER_SIZE, "find %s -type f -name '*%s' | sort", pathname, filetype);
            FILE *fp = popen(cmd, "r");
            if (fp) {
                size_t pos = 0;
                char line[256];
                while (fgets(line, sizeof(line), fp) && pos < BUFFER_SIZE - 256) {
                    line[strcspn(line, "\n")] = 0;
                    char *filename = basename(line);
                    pos += snprintf(file_list + pos, BUFFER_SIZE - pos, "%s\n", filename);
                }
                pclose(fp);
            }

            // Send file list to client
            if (strlen(file_list) == 0) {
                send(client_sock, "No files found", 14, 0);
            } else {
                send(client_sock, file_list, strlen(file_list), 0);
            }
        }
        close(client_sock);
    }
    close(server_sock);
    return 0;
}

// Create directories recursively
void create_directories(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, PATH_MAX, "%s", path);
    char *p;
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
    mkdir(tmp, S_IRWXU);
}