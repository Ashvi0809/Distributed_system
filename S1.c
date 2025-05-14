#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <libgen.h>
#include <limits.h>
#include <signal.h>
#include <errno.h>
#include <stdint.h>  // For uint64_t
#include <endian.h>  // For htobe64 and be64toh
#include <sys/time.h> // For timeout

#define BUFFER_SIZE 8192

// Global flag to control server shutdown
static volatile sig_atomic_t keep_running = 1;
// Server socket descriptor
static int server_sock = -1;
// Port numbers for S2, S3, S4 servers
static int PORT_S2, PORT_S3, PORT_S4;

// Signal handler for graceful shutdown
void signal_handler(int sig) {
    keep_running = 0;
    if (server_sock != -1) close(server_sock);
}

// Function prototypes
void prcclient(int client_sock);
int connect_to_server(int port);
void transfer_file_to_server(const char *filename, const char *dest_path, int server_port, int client_sock);
void download_file_from_server(const char *filepath, int server_port, int client_sock);
void create_directories(const char *path);
int receive_full(int sock, char *buffer, size_t size);

int main(int argc, char *argv[]) {
    // Validate command-line arguments
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <S1_port> <S2_port> <S3_port> <S4_port>\n", argv[0]);
        return 1;
    }

    // Parse port numbers
    int PORT_S1 = atoi(argv[1]);
    PORT_S2 = atoi(argv[2]);
    PORT_S3 = atoi(argv[3]);
    PORT_S4 = atoi(argv[4]);

    // Validate port range
    if (PORT_S1 < 1024 || PORT_S1 > 65535 || 
        PORT_S2 < 1024 || PORT_S2 > 65535 || 
        PORT_S3 < 1024 || PORT_S3 > 65535 || 
        PORT_S4 < 1024 || PORT_S4 > 65535) {
        fprintf(stderr, "Error: Ports must be between 1024 and 65535\n");
        return 1;
    }

    // Ensure unique ports
    if (PORT_S1 == PORT_S2 || PORT_S1 == PORT_S3 || PORT_S1 == PORT_S4 ||
        PORT_S2 == PORT_S3 || PORT_S2 == PORT_S4 || PORT_S3 == PORT_S4) {
        fprintf(stderr, "Error: All ports must be unique\n");
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
    server_addr.sin_port = htons(PORT_S1);

    // Bind socket to address
    if (bind(server_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_sock);
        return 1;
    }
    // Listen for incoming connections
    listen(server_sock, 5);
    printf("S1 listening on port %d...\n", PORT_S1);

    // Main server loop
    while (keep_running) {
        // Accept client connection
        int client_sock = accept(server_sock, (struct sockaddr*)&client_addr, &addr_len);
        if (client_sock < 0) {
            if (!keep_running) break;
            continue;
        }
        // Fork to handle client
        pid_t pid = fork();
        if (pid == 0) {
            // Child process
            close(server_sock);
            prcclient(client_sock);
            close(client_sock);
            exit(0);
        }
        // Parent process
        close(client_sock);
        // Reap terminated children
        while (waitpid(-1, NULL, WNOHANG) > 0);
    }
    close(server_sock);
    return 0;
}

// Process client commands
void prcclient(int client_sock) {
    char buffer[BUFFER_SIZE];
    while (1) {
        // Receive client command
        ssize_t received = recv(client_sock, buffer, BUFFER_SIZE - 1, 0);
        if (received <= 0) return;
        buffer[received] = '\0';

        // Parse command and parameters
        char command[20], param1[256] = {0};
        sscanf(buffer, "%s %[^\n]", command, param1);

        if (strcmp(command, "uploadf") == 0) {
            printf("S1: Received uploadf command: %s\n", buffer);
            char filename[256], dest_path[PATH_MAX];
            sscanf(buffer, "%*s %s %s", filename, dest_path);

            // Validate destination path
            if (strncmp(dest_path, "~S1/", 4) != 0) {
                send(client_sock, "Upload failed: Destination path must start with ~S1/", 51, 0);
                continue;
            }

            // Get home directory
            char *home = getenv("HOME");
            if (!home) {
                send(client_sock, "Upload failed: HOME environment variable not set", 48, 0);
                continue;
            }

            // Construct full destination path
            char full_dest_path[PATH_MAX];
            snprintf(full_dest_path, PATH_MAX, "%s/S1/%s", home, dest_path + 4);
            printf("S1: Full destination path: %s\n", full_dest_path);

            // Determine temporary file path
            char temp_path[PATH_MAX];
            if (full_dest_path[strlen(full_dest_path) - 1] == '/')
                snprintf(temp_path, PATH_MAX, "%s%s", full_dest_path, basename(filename));
            else
                snprintf(temp_path, PATH_MAX, "%s/%s", full_dest_path, basename(filename));
            printf("S1: Temporary file path: %s\n", temp_path);

            // Create necessary directories
            create_directories(dirname(strdup(temp_path)));

            // Remove existing file, if any
            remove(temp_path);
            // Open file for writing
            FILE *fp = fopen(temp_path, "wb");
            if (!fp) {
                char error_msg[BUFFER_SIZE];
                snprintf(error_msg, BUFFER_SIZE, "Upload failed: Cannot write file (%s)", strerror(errno));
                send(client_sock, error_msg, strlen(error_msg), 0);
                continue;
            }
            size_t bytes, total_bytes = 0;
            // Receive and write file data
            while ((bytes = recv(client_sock, buffer, BUFFER_SIZE, 0)) > 0) {
                if (fwrite(buffer, 1, bytes, fp) != bytes) {
                    fclose(fp);
                    remove(temp_path);
                    send(client_sock, "Upload failed: Error writing file", 33, 0);
                    continue;
                }
                total_bytes += bytes;
            }
            fclose(fp);
            if (total_bytes == 0) {
                remove(temp_path);
                send(client_sock, "Upload failed: No data received", 32, 0);
                continue;
            }
            printf("S1: Wrote %zu bytes to %s\n", total_bytes, temp_path);

            // Determine file extension
            char *ext = strrchr(filename, '.');
            if (ext && strcmp(ext, ".c") == 0) {
                // Store .c files locally
                send(client_sock, "Stored successfully", 20, 0);
                printf("S1: Stored %s\n", temp_path);
            } else {
                // Route other file types to appropriate servers
                int port = (ext && strcmp(ext, ".pdf") == 0) ? PORT_S2 :
                          (ext && strcmp(ext, ".txt") == 0) ? PORT_S3 :
                          (ext && strcmp(ext, ".zip") == 0) ? PORT_S4 : 0;
                if (port) {
                    transfer_file_to_server(temp_path, full_dest_path, port, client_sock);
                } else {
                    send(client_sock, "Upload failed: Unsupported file type", 37, 0);
                    remove(temp_path);
                }
            }
        } else if (strcmp(command, "downlf") == 0) {
            printf("S1: Received downlf command: %s\n", buffer);
            // Validate file path
            if (strlen(param1) == 0) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "No file path provided", 21, 0);
                continue;
            }

            // Construct file path
            char filepath[PATH_MAX];
            char *home = getenv("HOME");
            if (!home) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "HOME environment variable not set", 33, 0);
                continue;
            }
            if (strncmp(param1, "~S1/", 4) == 0) {
                snprintf(filepath, PATH_MAX, "%s/S1/%s", home, param1 + 4);
            } else {
                snprintf(filepath, PATH_MAX, "%s/S1/%s", home, param1);
            }
            printf("S1: Processing download request for %s\n", filepath);

            // Validate file extension
            char *ext = strrchr(filepath, '.');
            if (!ext || (strcmp(ext, ".c") != 0 && strcmp(ext, ".pdf") != 0 && 
                        strcmp(ext, ".txt") != 0 && strcmp(ext, ".zip") != 0)) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Only .c, .pdf, .txt, .zip supported", 35, 0);
                continue;
            }

            if (strcmp(ext, ".c") == 0) {
                // Handle .c files locally
                struct stat statbuf;
                if (stat(filepath, &statbuf) == 0 && S_ISREG(statbuf.st_mode)) {
                    uint64_t file_size = statbuf.st_size;
                    uint64_t net_size = htobe64(file_size);
                    printf("S1: Sending file size for %s: %lu bytes\n", filepath, file_size);
                    if (send(client_sock, (char*)&net_size, sizeof(net_size), 0) < 0) {
                        printf("S1: Failed to send file size to client\n");
                        continue;
                    }
                    // Open and send file
                    FILE *fp = fopen(filepath, "rb");
                    if (fp) {
                        size_t bytes, total_sent = 0;
                        while ((bytes = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
                            if (send(client_sock, buffer, bytes, 0) < 0) {
                                printf("S1: Send error after %zu bytes\n", total_sent);
                                break;
                            }
                            total_sent += bytes;
                            printf("S1: Sent %zu bytes, total %zu/%lu\n", bytes, total_sent, file_size);
                        }
                        fclose(fp);
                        printf("S1: Sent %s to client (%zu bytes)\n", filepath, total_sent);
                    } else {
                        uint64_t zero = 0;
                        send(client_sock, (char*)&zero, sizeof(zero), 0);
                        send(client_sock, "Error opening file", 18, 0);
                    }
                } else {
                    uint64_t zero = 0;
                    send(client_sock, (char*)&zero, sizeof(zero), 0);
                    send(client_sock, "File not found", 14, 0);
                }
            } else {
                // Route download to other servers
                int port = (strcmp(ext, ".pdf") == 0) ? PORT_S2 :
                          (strcmp(ext, ".txt") == 0) ? PORT_S3 :
                          (strcmp(ext, ".zip") == 0) ? PORT_S4 : 0;
                download_file_from_server(filepath, port, client_sock);
            }
        } else if (strcmp(command, "removef") == 0) {
            printf("S1: Received removef command: %s\n", buffer);
            // Construct file path
            char filepath[PATH_MAX];
            char *home = getenv("HOME");
            if (!home) {
                send(client_sock, "Remove failed: HOME environment variable not set", 48, 0);
                continue;
            }
            if (strncmp(param1, "~S1/", 4) == 0) {
                snprintf(filepath, PATH_MAX, "%s/S1/%s", home, param1 + 4);
            } else {
                snprintf(filepath, PATH_MAX, "%s/S1/%s", home, param1);
            }

            // Get file extension
            char *ext = strrchr(filepath, '.');
            if (!ext) {
                send(client_sock, "Remove failed: No file extension", 32, 0);
                continue;
            }
            ext++;

            if (strcmp(ext, "c") == 0) {
                // Remove .c files locally
                struct stat statbuf;
                if (stat(filepath, &statbuf) == 0) {
                    if (S_ISREG(statbuf.st_mode)) {
                        if (remove(filepath) == 0) {
                            send(client_sock, "File removed successfully", 25, 0);
                            printf("S1: Removed %s\n", filepath);
                        } else {
                            send(client_sock, "Remove failed: Permission denied", 32, 0);
                        }
                    } else {
                        send(client_sock, "Remove failed: Not a regular file", 33, 0);
                    }
                } else {
                    send(client_sock, "Remove failed: File not found", 29, 0);
                }
            } else if (strcmp(ext, "pdf") == 0 || strcmp(ext, "txt") == 0) {
                // Forward remove request to S2 or S3
                int port = (strcmp(ext, "pdf") == 0) ? PORT_S2 : PORT_S3;
                int sock = connect_to_server(port);
                if (sock < 0) {
                    send(client_sock, "Remove failed: Cannot connect to server", 39, 0);
                    continue;
                }

                // Adjust path for target server
                char server_dir[4];
                snprintf(server_dir, 4, "S%d", (port == PORT_S2) ? 2 : 3);
                char adjusted_path[PATH_MAX];
                snprintf(adjusted_path, PATH_MAX, "%s/%s/%s", home, server_dir, param1 + 4);

                char remove_cmd[BUFFER_SIZE];
                snprintf(remove_cmd, BUFFER_SIZE, "removef %s", adjusted_path);
                send(sock, remove_cmd, strlen(remove_cmd), 0);

                // Receive and forward server response
                memset(buffer, 0, BUFFER_SIZE);
                ssize_t recv_bytes = recv(sock, buffer, BUFFER_SIZE - 1, 0);
                close(sock);
                if (recv_bytes > 0) {
                    buffer[recv_bytes] = '\0';
                    send(client_sock, buffer, strlen(buffer), 0);
                } else {
                    send(client_sock, "Remove failed: No response from server", 38, 0);
                }
            } else {
                send(client_sock, "Remove failed: Unsupported file type", 36, 0);
            }
        } else if (strcmp(command, "downltar") == 0) {
            printf("S1: Received downltar command: %s\n", buffer);
            // Validate file type
            if (strlen(param1) == 0) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Download failed: No file type provided", 38, 0);
                continue;
            }
            if (strcmp(param1, ".c") != 0 && strcmp(param1, ".pdf") != 0 && strcmp(param1, ".txt") != 0) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Download failed: Invalid file type", 34, 0);
                continue;
            }

            // Construct tar file path
            char tar_path[PATH_MAX];
            char *home = getenv("HOME");
            if (!home) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Download failed: HOME environment variable not set", 50, 0);
                continue;
            }
            snprintf(tar_path, PATH_MAX, "%s/S1/temp/%s.tar", home,
                     strcmp(param1, ".c") == 0 ? "cfiles" :
                     strcmp(param1, ".pdf") == 0 ? "pdffiles" : "textfiles");
            create_directories(dirname(strdup(tar_path)));

            if (strcmp(param1, ".c") == 0) {
                // Create tar of local .c files
                char cmd[BUFFER_SIZE];
                snprintf(cmd, BUFFER_SIZE, "cd %s/S1 && find * -type f -name '*.c' | tar -cf %s -T -", home, tar_path);
                int ret = system(cmd);
                if (ret != 0) {
                    uint64_t zero = 0;
                    send(client_sock, (char*)&zero, sizeof(zero), 0);
                    send(client_sock, "Download failed: No .c files found or tar creation failed", 56, 0);
                    remove(tar_path);
                    continue;
                }
            } else {
                // Request tar from S2 or S3
                int port = strcmp(param1, ".pdf") == 0 ? PORT_S2 : PORT_S3;
                int sock = connect_to_server(port);
                if (sock < 0) {
                    uint64_t zero = 0;
                    send(client_sock, (char*)&zero, sizeof(zero), 0);
                    send(client_sock, "Download failed: Cannot connect to server", 41, 0);
                    continue;
                }

                char downltar_cmd[BUFFER_SIZE];
                snprintf(downltar_cmd, BUFFER_SIZE, "downltar %s", param1);
                send(sock, downltar_cmd, strlen(downltar_cmd), 0);

                // Receive tar file size
                uint64_t net_file_size;
                if (receive_full(sock, (char*)&net_file_size, sizeof(net_file_size)) < 0) {
                    uint64_t zero = 0;
                    send(client_sock, (char*)&zero, sizeof(zero), 0);
                    send(client_sock, "Download failed: Error receiving file size", 42, 0);
                    close(sock);
                    continue;
                }
                uint64_t file_size = be64toh(net_file_size);

                if (file_size == 0) {
                    // Handle error response
                    memset(buffer, 0, BUFFER_SIZE);
                    ssize_t recv_bytes = recv(sock, buffer, BUFFER_SIZE - 1, 0);
                    close(sock);
                    if (recv_bytes > 0) {
                        buffer[recv_bytes] = '\0';
                        uint64_t zero = 0;
                        send(client_sock, (char*)&zero, sizeof(zero), 0);
                        send(client_sock, buffer, strlen(buffer), 0);
                    } else {
                        uint64_t zero = 0;
                        send(client_sock, (char*)&zero, sizeof(zero), 0);
                        send(client_sock, "Download failed: No response from server", 39, 0);
                    }
                    continue;
                }

                // Save tar file locally
                FILE *fp = fopen(tar_path, "wb");
                if (!fp) {
                    uint64_t zero = 0;
                    send(client_sock, (char*)&zero, sizeof(zero), 0);
                    send(client_sock, "Download failed: Cannot create temp file on S1", 46, 0);
                    close(sock);
                    continue;
                }

                size_t total_received = 0;
                while (total_received < file_size) {
                    size_t to_receive = file_size - total_received;
                    if (to_receive > BUFFER_SIZE) to_receive = BUFFER_SIZE;
                    ssize_t recv_bytes = recv(sock, buffer, to_receive, 0);
                    if (recv_bytes <= 0) break;
                    fwrite(buffer, 1, recv_bytes, fp);
                    total_received += recv_bytes;
                }
                fclose(fp);
                close(sock);
                if (total_received != file_size) {
                    uint64_t zero = 0;
                    send(client_sock, (char*)&zero, sizeof(zero), 0);
                    send(client_sock, "Download failed: Transfer interrupted", 37, 0);
                    remove(tar_path);
                    continue;
                }
            }

            // Send tar file to client
            struct stat statbuf;
            if (stat(tar_path, &statbuf) != 0) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Download failed: Tar file not found on S1", 41, 0);
                remove(tar_path);
                continue;
            }
            uint64_t file_size = statbuf.st_size;
            uint64_t net_size = htobe64(file_size);
            send(client_sock, (char*)&net_size, sizeof(net_size), 0);

            FILE *fp = fopen(tar_path, "rb");
            if (!fp) {
                uint64_t zero = 0;
                send(client_sock, (char*)&zero, sizeof(zero), 0);
                send(client_sock, "Download failed: Cannot open tar file on S1", 43, 0);
                remove(tar_path);
                continue;
            }
            size_t bytes;
            while ((bytes = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
                send(client_sock, buffer, bytes, 0);
            }
            fclose(fp);
            printf("S1: Sent %s to client\n", tar_path);
            remove(tar_path);
        } else if (strcmp(command, "dispfnames") == 0) {
            printf("S1: Received dispfnames command: %s\n", buffer);
            // Validate path
            if (strlen(param1) == 0) {
                send(client_sock, "No files found", 14, 0);
                continue;
            }

            // Construct directory path
            char pathname[PATH_MAX];
            char *home = getenv("HOME");
            if (!home) {
                send(client_sock, "No files found: HOME environment variable not set", 49, 0);
                continue;
            }
            if (strncmp(param1, "~S1/", 4) == 0) {
                snprintf(pathname, PATH_MAX, "%s/S1/%s", home, param1 + 4);
            } else {
                snprintf(pathname, PATH_MAX, "%s/S1/%s", home, param1);
            }

            // Verify directory exists
            struct stat statbuf;
            if (stat(pathname, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode)) {
                send(client_sock, "No files found", 14, 0);
                continue;
            }

            char file_list[BUFFER_SIZE] = {0};
            char temp_list[BUFFER_SIZE] = {0};
            char *types[] = {".c", ".pdf", ".txt", ".zip"};
            int ports[] = {0, PORT_S2, PORT_S3, PORT_S4};

            // Collect file names for each type
            for (int i = 0; i < 4; i++) {
                char current_list[BUFFER_SIZE] = {0};
                if (i == 0) {
                    // Local .c files
                    char cmd[BUFFER_SIZE];
                    snprintf(cmd, BUFFER_SIZE, "find %s -type f -name '*%s' | sort", pathname, types[i]);
                    FILE *fp = popen(cmd, "r");
                    if (fp) {
                        size_t pos = 0;
                        char line[256];
                        while (fgets(line, sizeof(line), fp) && pos < BUFFER_SIZE - 256) {
                            line[strcspn(line, "\n")] = 0;
                            char *filename = basename(line);
                            pos += snprintf(current_list + pos, BUFFER_SIZE - pos, "%s\n", filename);
                        }
                        pclose(fp);
                    }
                } else {
                    // Request file names from other servers
                    int sock = connect_to_server(ports[i]);
                    if (sock >= 0) {
                        char adjusted_path[PATH_MAX];
                        snprintf(adjusted_path, PATH_MAX, "%s/S%d/%s", home, i + 1, param1 + 4);
                        char disp_cmd[BUFFER_SIZE];
                        snprintf(disp_cmd, BUFFER_SIZE, "dispfnames %s %s", adjusted_path, types[i]);
                        send(sock, disp_cmd, strlen(disp_cmd), 0);

                        memset(temp_list, 0, BUFFER_SIZE);
                        ssize_t recv_bytes = recv(sock, temp_list, BUFFER_SIZE - 1, 0);
                        if (recv_bytes > 0) {
                            temp_list[recv_bytes] = '\0';
                            if (strcmp(temp_list, "No files found") != 0) {
                                strncpy(current_list, temp_list, BUFFER_SIZE - 1);
                            }
                        }
                        close(sock);
                    }
                }
                if (strlen(current_list) > 0) {
                    strncat(file_list, current_list, BUFFER_SIZE - strlen(file_list) - 1);
                }
            }

            // Send file list to client
            if (strlen(file_list) == 0) {
                send(client_sock, "No files found", 14, 0);
            } else {
                send(client_sock, file_list, strlen(file_list), 0);
            }
        }
    }
}

// Connect to another server
int connect_to_server(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr = {AF_INET, htons(port), inet_addr("127.0.0.1")};
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

// Transfer file to another server
void transfer_file_to_server(const char *filename, const char *dest_path, int server_port, int client_sock) {
    // Connect to target server
    int sock = connect_to_server(server_port);
    if (sock < 0) {
        send(client_sock, "Upload failed: Server connection error", 38, 0);
        remove(filename);
        return;
    }

    char buffer[BUFFER_SIZE];
    char adjusted_path[PATH_MAX];
    char *home = getenv("HOME");
    if (!home) {
        send(client_sock, "Upload failed: HOME environment variable not set", 48, 0);
        close(sock);
        remove(filename);
        return;
    }

    // Determine server directory
    const char *server_dir = (server_port == PORT_S2) ? "S2" :
                            (server_port == PORT_S3) ? "S3" :
                            (server_port == PORT_S4) ? "S4" : NULL;
    const char *suffix = dest_path + strlen(home) + 3;
    if (suffix[0] == '/' || dest_path[strlen(dest_path) - 1] == '/')
        snprintf(adjusted_path, PATH_MAX, "%s/%s%s", home, server_dir, suffix);
    else
        snprintf(adjusted_path, PATH_MAX, "%s/%s/%s", home, server_dir, suffix);
    printf("S1: Transferring to %s on port %d\n", adjusted_path, server_port);

    // Send upload command
    char *filename_copy = strdup(filename);
    snprintf(buffer, BUFFER_SIZE, "uploadf %s %s", basename(filename_copy), adjusted_path);
    free(filename_copy);
    ssize_t sent = send(sock, buffer, strlen(buffer), 0);
    if (sent < 0) {
        send(client_sock, "Upload failed: Failed to send command to server", 47, 0);
        close(sock);
        remove(filename);
        return;
    }
    printf("S1: Sent command to server on port %d: %s\n", server_port, buffer);

    // Open and send file
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        send(client_sock, "Upload failed: File not accessible", 35, 0);
        close(sock);
        remove(filename);
        return;
    }

    size_t bytes, total_bytes = 0;
    while ((bytes = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
        ssize_t sent_bytes = send(sock, buffer, bytes, 0);
        if (sent_bytes != bytes) {
            fclose(fp);
            close(sock);
            send(client_sock, "Upload failed: Error sending file to server", 43, 0);
            remove(filename);
            return;
        }
        total_bytes += sent_bytes;
    }
    fclose(fp);
    printf("S1: Sent %zu bytes to server on port %d\n", total_bytes, server_port);

    // Brief delay to ensure server processes data
    usleep(100000); // 100ms delay
    shutdown(sock, SHUT_WR);

    // Receive server response
    memset(buffer, 0, BUFFER_SIZE);
    ssize_t recv_bytes = recv(sock, buffer, BUFFER_SIZE - 1, 0);
    if (recv_bytes > 0) {
        buffer[recv_bytes] = '\0';
        send(client_sock, buffer, strlen(buffer), 0);
        printf("S1: Transfer to server on port %d completed: %s\n", server_port, buffer);
    } else {
        send(client_sock, "Upload failed: No response from server", 38, 0);
    }
    remove(filename);
    close(sock);
}

// Download file from another server and forward to client
void download_file_from_server(const char *filepath, int server_port, int client_sock) {
    // Connect to target server
    int sock = connect_to_server(server_port);
    if (sock < 0) {
        uint64_t zero = 0;
        send(client_sock, (char*)&zero, sizeof(zero), 0);
        send(client_sock, "Server connection error", 23, 0);
        printf("S1: Failed to connect to server on port %d\n", server_port);
        return;
    }

    // Set receive timeout (5 seconds)
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char buffer[BUFFER_SIZE];
    char adjusted_path[PATH_MAX];
    char *home = getenv("HOME");
    // Adjust path for target server
    const char *server_dir = (server_port == PORT_S2) ? "S2" :
                            (server_port == PORT_S3) ? "S3" :
                            (server_port == PORT_S4) ? "S4" : NULL;
    snprintf(adjusted_path, PATH_MAX, "%s/%s%s", home, server_dir, filepath + strlen(home) + 3);
    snprintf(buffer, BUFFER_SIZE, "downlf %s", adjusted_path);
    if (send(sock, buffer, strlen(buffer), 0) < 0) {
        uint64_t zero = 0;
        send(client_sock, (char*)&zero, sizeof(zero), 0);
        send(client_sock, "Failed to send command to server", 31, 0);
        printf("S1: Failed to send command to port %d\n", server_port);
        close(sock);
        return;
    }
    printf("S1: Sent command to server on port %d: %s\n", server_port, buffer);

    // Receive file size
    uint64_t net_file_size;
    if (receive_full(sock, (char*)&net_file_size, sizeof(net_file_size)) < 0) {
        uint64_t zero = 0;
        send(client_sock, (char*)&zero, sizeof(zero), 0);
        send(client_sock, "Error receiving file size", 25, 0);
        printf("S1: Failed to receive file size from port %d\n", server_port);
        close(sock);
        return;
    }
    uint64_t file_size = be64toh(net_file_size);
    printf("S1: Received file size from port %d: %lu bytes\n", server_port, file_size);

    // Reset timeout for data transfer
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Send file size to client
    if (send(client_sock, (char*)&net_file_size, sizeof(net_file_size), 0) < 0) {
        printf("S1: Failed to send file size to client\n");
        close(sock);
        return;
    }
    printf("S1: Sent file size to client: %lu bytes\n", file_size);

    if (file_size > 0) {
        // Transfer file data
        size_t total_received = 0;
        while (total_received < file_size) {
            size_t to_receive = file_size - total_received;
            if (to_receive > BUFFER_SIZE) to_receive = BUFFER_SIZE;
            ssize_t bytes = recv(sock, buffer, to_receive, 0);
            if (bytes <= 0) {
                printf("S1: Receive error from port %d after %zu bytes\n", server_port, total_received);
                break;
            }
            if (send(client_sock, buffer, bytes, 0) < 0) {
                printf("S1: Send error to client after %zu bytes\n", total_received);
                break;
            }
            total_received += bytes;
            printf("S1: Transferred %zd bytes from port %d, total %zu/%lu\n", bytes, server_port, total_received, file_size);
        }
        if (total_received == file_size) {
            printf("S1: Successfully received %s from port %d and sent to client\n", adjusted_path, server_port);
        } else {
            printf("S1: Incomplete transfer from port %d, %zu/%lu bytes\n", server_port, total_received, file_size);
        }
    } else {
        // Handle error response
        ssize_t bytes = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes > 0) {
            buffer[bytes] = '\0';
            send(client_sock, buffer, bytes, 0);
            printf("S1: Received error from port %d: %s\n", server_port, buffer);
        } else {
            send(client_sock, "No response from server", 23, 0);
            printf("S1: No response from port %d\n", server_port);
        }
    }
    close(sock);
}

// Create directories recursively
void create_directories(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, PATH_MAX, "%s", path);
    char *p;
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
    mkdir(tmp, S_IRWXU);
}

// Receive exact number of bytes
int receive_full(int sock, char *buffer, size_t size) {
    size_t received = 0;
    while (received < size) {
        ssize_t bytes = recv(sock, buffer + received, size - received, 0);
        if (bytes <= 0) return -1;  // Error or connection closed
        received += bytes;
    }
    return 0;  // Success
}