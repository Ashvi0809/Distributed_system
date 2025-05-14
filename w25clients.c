#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <libgen.h>
#include <limits.h>
#include <stdint.h>  // For uint64_t
#include <endian.h>  // For be64toh
#include <sys/time.h> // For timeout

#define BUFFER_SIZE 8192

// Function to receive exact number of bytes from socket
int receive_full(int sock, char *buffer, size_t size);

int main(int argc, char *argv[]) {
    // Validate command-line arguments
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <S1_port>\n", argv[0]);
        return 1;
    }

    // Parse and validate port number
    int PORT_S1 = atoi(argv[1]);
    if (PORT_S1 < 1024 || PORT_S1 > 65535) {
        fprintf(stderr, "Error: Port must be between 1024 and 65535\n");
        return 1;
    }

    // Create client socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Configure server address
    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = inet_addr("127.0.0.1"),
        .sin_port = htons(PORT_S1)
    };

    // Connect to server
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Failed to connect to S1");
        close(sock);
        return 1;
    }
    printf("Connected to S1 on port %d. Enter commands:\n", PORT_S1);

    char buffer[BUFFER_SIZE];
    // Main client loop
    while (1) {
        // Prompt for user input
        printf("w25clients$ ");
        fgets(buffer, BUFFER_SIZE, stdin);
        buffer[strcspn(buffer, "\n")] = 0;

        // Parse command and parameters
        char command[20], param1[256] = {0};
        sscanf(buffer, "%s %[^\n]", command, param1);

        if (strcmp(command, "uploadf") == 0) {
            printf("Client: Sending uploadf command: %s\n", buffer);
            char param2[256] = {0};
            sscanf(buffer, "%*s %s %s", param1, param2);

            // Validate destination path
            if (strncmp(param2, "~S1/", 4) != 0) {
                printf("Error: Destination path must start with ~S1/\n");
                continue;
            }

            // Construct full source file path
            char full_path[PATH_MAX];
            if (param1[0] != '/') {
                char cwd[PATH_MAX];
                getcwd(cwd, PATH_MAX);
                snprintf(full_path, PATH_MAX, "%s/%s", cwd, param1);
            } else {
                strncpy(full_path, param1, PATH_MAX);
            }
            printf("Client: Source file path: %s\n", full_path);
            printf("Client: Destination path: %s\n", param2);

            // Open source file
            FILE *fp = fopen(full_path, "rb");
            if (!fp) {
                printf("Error: File %s not found\n", full_path);
                continue;
            }

            // Send upload command
            char updated_command[BUFFER_SIZE];
            snprintf(updated_command, BUFFER_SIZE, "uploadf %s %s", param1, param2);
            send(sock, updated_command, strlen(updated_command), 0);

            // Send file data
            size_t bytes;
            while ((bytes = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
                send(sock, buffer, bytes, 0);
            }
            fclose(fp);
            // Signal end of data
            shutdown(sock, SHUT_WR);

            // Receive server response
            memset(buffer, 0, BUFFER_SIZE);
            ssize_t received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
            if (received > 0) {
                buffer[received] = '\0';
                printf("%s\n", buffer);
            } else {
                printf("Error: No response from S1\n");
                break;
            }

            // Reconnect for next command
            int new_sock = socket(AF_INET, SOCK_STREAM, 0);
            if (connect(new_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
                perror("Failed to reconnect to S1");
                close(sock);
                sock = new_sock;
                continue;
            }
            close(sock);
            sock = new_sock;
        } else if (strcmp(command, "downlf") == 0) {
            printf("Client: Sending downlf command: %s\n", buffer);
            // Validate file path
            if (strlen(param1) == 0) {
                printf("Error: Please provide a file path (e.g., ~S1/folder1/sample.txt)\n");
                continue;
            }

            // Send download command
            char updated_command[BUFFER_SIZE];
            snprintf(updated_command, BUFFER_SIZE, "downlf %s", param1);
            if (send(sock, updated_command, strlen(updated_command), 0) < 0) {
                printf("Error: Failed to send command\n");
                continue;
            }

            // Set receive timeout (5 seconds)
            struct timeval tv;
            tv.tv_sec = 5;
            tv.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            // Receive file size
            uint64_t net_file_size;
            if (receive_full(sock, (char*)&net_file_size, sizeof(net_file_size)) < 0) {
                printf("Error: Failed to receive file size\n");
                // Reset timeout
                tv.tv_sec = 0;
                tv.tv_usec = 0;
                setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                continue;
            }
            uint64_t file_size = be64toh(net_file_size);
            printf("Client: Received file size: %lu bytes\n", file_size);

            // Reset timeout for data transfer
            tv.tv_sec = 0;
            tv.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            if (file_size > 0) {
                // Prepare output file
                char filename[256];
                strcpy(filename, basename(param1));
                FILE *fp = fopen(filename, "wb");
                if (!fp) {
                    printf("Error: Cannot create file %s in PWD\n", filename);
                    // Consume data to keep connection alive
                    size_t total_received = 0;
                    while (total_received < file_size) {
                        size_t to_receive = file_size - total_received;
                        if (to_receive > BUFFER_SIZE) to_receive = BUFFER_SIZE;
                        ssize_t bytes = recv(sock, buffer, to_receive, 0);
                        if (bytes <= 0) break;
                        total_received += bytes;
                    }
                    continue;
                }
                // Receive and write file data
                size_t total_received = 0;
                while (total_received < file_size) {
                    size_t to_receive = file_size - total_received;
                    if (to_receive > BUFFER_SIZE) to_receive = BUFFER_SIZE;
                    ssize_t bytes = recv(sock, buffer, to_receive, 0);
                    if (bytes <= 0) {
                        printf("Client: Receive error after %zu bytes\n", total_received);
                        break;
                    }
                    fwrite(buffer, 1, bytes, fp);
                    total_received += bytes;
                    printf("Client: Received %zd bytes, total %zu/%lu\n", bytes, total_received, file_size);
                }
                fclose(fp);
                // Report download status
                if (total_received == file_size) {
                    printf("Download of %s completed successfully\n", filename);
                } else {
                    printf("Error: Download incomplete, received %zu/%lu bytes\n", total_received, file_size);
                }
            } else {
                // Handle server error
                memset(buffer, 0, BUFFER_SIZE);
                ssize_t received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
                if (received > 0) {
                    buffer[received] = '\0';
                    printf("Server error: %s\n", buffer);
                } else {
                    printf("Error: No response from S1\n");
                }
            }
        } else if (strcmp(command, "removef") == 0) {
            printf("Client: Sending removef command: %s\n", buffer);
            // Validate file path
            if (strlen(param1) == 0) {
                printf("Error: Please provide a file path (e.g., ~S1/folder1/sample.txt)\n");
                continue;
            }

            // Send remove command
            char updated_command[BUFFER_SIZE];
            snprintf(updated_command, BUFFER_SIZE, "removef %s", param1);
            send(sock, updated_command, strlen(updated_command), 0);

            // Receive server response
            memset(buffer, 0, BUFFER_SIZE);
            ssize_t received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
            if (received > 0) {
                buffer[received] = '\0';
                printf("%s\n", buffer);
            } else {
                printf("Error: No response from S1\n");
            }
        } else if (strcmp(command, "downltar") == 0) {
            printf("Client: Sending downltar command: %s\n", buffer);
            // Validate file type
            if (strlen(param1) == 0) {
                printf("Error: Please provide a file type (.c, .pdf, or .txt)\n");
                continue;
            }
            if (strcmp(param1, ".c") != 0 && strcmp(param1, ".pdf") != 0 && strcmp(param1, ".txt") != 0) {
                printf("Error: File type must be .c, .pdf, or .txt\n");
                continue;
            }

            // Send download tar command
            char updated_command[BUFFER_SIZE];
            snprintf(updated_command, BUFFER_SIZE, "downltar %s", param1);
            send(sock, updated_command, strlen(updated_command), 0);

            // Set receive timeout (5 seconds)
            struct timeval tv;
            tv.tv_sec = 5;
            tv.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            // Receive tar file size
            uint64_t net_file_size;
            if (receive_full(sock, (char*)&net_file_size, sizeof(net_file_size)) < 0) {
                printf("Error: Failed to receive file size\n");
                // Reset timeout
                tv.tv_sec = 0;
                tv.tv_usec = 0;
                setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                continue;
            }
            uint64_t file_size = be64toh(net_file_size);
            printf("Client: Received file size: %lu bytes\n", file_size);

            // Reset timeout for data transfer
            tv.tv_sec = 0;
            tv.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            if (file_size > 0) {
                // Prepare tar file name
                char filename[256];
                snprintf(filename, 256, "%s.tar", 
                         strcmp(param1, ".c") == 0 ? "cfiles" : 
                         strcmp(param1, ".pdf") == 0 ? "pdffiles" : "textfiles");
                FILE *fp = fopen(filename, "wb");
                if (!fp) {
                    printf("Error: Cannot create file %s in PWD\n", filename);
                    // Consume data to keep connection alive
                    size_t total_received = 0;
                    while (total_received < file_size) {
                        size_t to_receive = file_size - total_received;
                        if (to_receive > BUFFER_SIZE) to_receive = BUFFER_SIZE;
                        ssize_t bytes = recv(sock, buffer, to_receive, 0);
                        if (bytes <= 0) break;
                        total_received += bytes;
                    }
                    continue;
                }
                // Receive and write tar file
                size_t total_received = 0;
                while (total_received < file_size) {
                    size_t to_receive = file_size - total_received;
                    if (to_receive > BUFFER_SIZE) to_receive = BUFFER_SIZE;
                    ssize_t bytes = recv(sock, buffer, to_receive, 0);
                    if (bytes <= 0) {
                        printf("Client: Receive error after %zu bytes\n", total_received);
                        break;
                    }
                    fwrite(buffer, 1, bytes, fp);
                    total_received += bytes;
                    printf("Client: Received %zd bytes, total %zu/%lu\n", bytes, total_received, file_size);
                }
                fclose(fp);
                // Report download status
                if (total_received == file_size) {
                    printf("Download of %s completed successfully\n", filename);
                } else {
                    printf("Error: Download incomplete, received %zu/%lu bytes\n", total_received, file_size);
                }
            } else {
                // Handle server error
                memset(buffer, 0, BUFFER_SIZE);
                ssize_t received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
                if (received > 0) {
                    buffer[received] = '\0';
                    printf("Server error: %s\n", buffer);
                } else {
                    printf("Error: No response from S1\n");
                }
            }
        } else if (strcmp(command, "dispfnames") == 0) {
            printf("Client: Sending dispfnames command: %s\n", buffer);
            // Validate path
            if (strlen(param1) == 0) {
                printf("Error: Please provide a pathname (e.g., ~S1/folder1)\n");
                continue;
            }

            // Send display names command
            char updated_command[BUFFER_SIZE];
            snprintf(updated_command, BUFFER_SIZE, "dispfnames %s", param1);
            send(sock, updated_command, strlen(updated_command), 0);

            // Receive server response
            memset(buffer, 0, BUFFER_SIZE);
            ssize_t received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
            if (received <= 0) {
                printf("Error: No response from S1\n");
                continue;
            }
            buffer[received] = '\0';

            // Display file list
            if (strcmp(buffer, "No files found") == 0) {
                printf("No files found in %s\n", param1);
            } else {
                printf("Files in %s:\n%s", param1, buffer);
            }
        } else if (strcmp(command, "exit") == 0) {
            printf("Client: Sending exit command\n");
            close(sock);
            return 0;
        } else {
            printf("Client: Unknown command: %s\n", command);
        }
    }
    close(sock);
    return 0;
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