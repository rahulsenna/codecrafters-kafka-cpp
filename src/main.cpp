#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

inline void write_int32_be(uint8_t **dest, int32_t value)
{
    (*dest)[0] = (value >> 24) & 0xFF;
    (*dest)[1] = (value >> 16) & 0xFF;
    (*dest)[2] = (value >> 8) & 0xFF;
    (*dest)[3] = value & 0xFF;
    (*dest) += 4;
}

inline void write_int16_be(uint8_t **dest, int16_t value)
{
    (*dest)[0] = (value >> 8) & 0xFF;
    (*dest)[1] = value & 0xFF;
    (*dest) += 2;
}

inline void copy_bytes(uint8_t **dest, char *src, int cnt)
{
    for (int i = 0; i < cnt; ++i)
    {
    	*(*dest)++ =  src[i];
    }
}

int main(int argc, char* argv[])
{
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";
    
    while (1) 
    {
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
        if (fork() != 0)
            continue;
        std::cout << "Client connected\n";

        char req_buf[1024];
        uint8_t resp_buf[1024];
        while (size_t bytes_read = read(client_fd, req_buf, 1024))
        {

            req_buf[bytes_read] = 0;
            memset(resp_buf, 0, 1024);
            uint8_t *ptr = resp_buf + 4;
            constexpr int cor_id_offset = 8;
            copy_bytes(&ptr, &req_buf[cor_id_offset], 4);

            constexpr int req_api_offset = 4;
            int16_t request_api_key = ((uint8_t)req_buf[req_api_offset + 0] |
                                       (uint8_t)req_buf[req_api_offset + 1]);

            int16_t request_api_version = ((uint8_t)req_buf[req_api_offset + 2] |
                                           (uint8_t)req_buf[req_api_offset + 3]);

            int error_code = 35; // (UNSUPPORTED_VERSION)
            if (request_api_version <= 4)
            {
                error_code = 0; // (NO_ERROR)
            }
            if (request_api_key == 0x004b)
            {
                error_code = 3; // (UNKNOWN_TOPIC)
            }

            // https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
            constexpr int8_t TAG_BUFFER = 0;
            
            if (request_api_key == 0x004b) // DescribeTopicPartitions
            {
                constexpr int client_id_offset = cor_id_offset + 4;
                int client_id_len = ((uint8_t)req_buf[client_id_offset] | (uint8_t)req_buf[client_id_offset + 1]) + /* TAG_BUFFER BYTE */ 1 + /* LENGTH BYTES */ 2;
                int topic_offset = client_id_offset+client_id_len;
                *ptr++ = TAG_BUFFER;
                write_int32_be(&ptr, 0);  // throttle_time_ms
                *ptr++ = req_buf[topic_offset++]; // topic.length
                
                write_int16_be(&ptr, error_code);

                int topic_name_len = (uint8_t)req_buf[topic_offset];
                copy_bytes(&ptr, &req_buf[topic_offset], topic_name_len);
                for (int i = 0; i < 16; ++i) // topic_id
                {
                    *ptr++ = 0;
                }
                *ptr++ = 0; // topic.is_internal
                *ptr++ = 1; // topic.partition
                write_int32_be(&ptr, 0x00000df8); // Topic Authorized Operations
                *ptr++ = TAG_BUFFER;
                *ptr++ = 0xFF; // Next Cursor (0xff, indicating a null value.)
                *ptr++ = TAG_BUFFER;
            }

            if (request_api_key == 0x0012) // API Versions
            {
                write_int16_be(&ptr, error_code);
                int8_t num_api_keys = 1 + 2; // 1 + # of elements because 0 is null array and 1 is empty array
                *ptr++ = num_api_keys;
                copy_bytes(&ptr, &req_buf[req_api_offset], 2); // api_key
                write_int16_be(&ptr, 0);                       // min_ver
                write_int16_be(&ptr, request_api_version);     // max_ver
                *ptr++ = TAG_BUFFER;                      // array_end

                write_int16_be(&ptr, 75); // api_key ( DescribeTopicPartitions )
                write_int16_be(&ptr, 0);  // min_ver
                write_int16_be(&ptr, 0);  // max_ver
                *ptr++ = TAG_BUFFER; // array_end

                write_int32_be(&ptr, 0); // throttle_time_ms
                *ptr++ = TAG_BUFFER;
            }


                int message_size = ptr - resp_buf;
                ptr = resp_buf;
                write_int32_be(&ptr, message_size - 4);

                write(client_fd, resp_buf, message_size);
            }

        close(client_fd);
    }

    close(server_fd);
    return 0;
}

void DescribeTopicPartitionsResponse(uint8_t *ptr)
{
    
}