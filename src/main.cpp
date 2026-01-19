#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

int main(int argc, char* argv[]) 
{
    // 禁用输出缓冲
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) 
    {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // 由于测试程序会频繁重启，设置 SO_REUSEADDR 避免 "Address already in use" 错误
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) 
    {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) 
    {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) 
    {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // 调试信息会在运行测试时显示
    std::cerr << "Logs from your program will appear here!\n";
    
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";
    
    // 循环处理同一连接上的多个请求
    while (true) 
    {
        // 从客户端读取请求
        char buffer[1024];
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        
        // 如果读取失败或连接关闭，退出循环
        if (bytes_read <= 0)
        {
            break;
        }
        
        // 解析请求头 v2
        // 请求结构:
        //   message_size (4字节) + request_api_key (2字节) + request_api_version (2字节) + correlation_id (4字节)
        
        // request_api_version 在偏移量 6 处 (4 + 2)
        int16_t request_api_version_net;
        memcpy(&request_api_version_net, buffer + 6, sizeof(request_api_version_net));
        int16_t request_api_version = ntohs(request_api_version_net);
        
        // correlation_id 在偏移量 8 处 (4 + 2 + 2)
        int32_t correlation_id;
        memcpy(&correlation_id, buffer + 8, sizeof(correlation_id));
        // correlation_id 已经是网络字节序（大端），回显时无需转换
        
        // 确定 error_code
        // 支持的 ApiVersions 版本: 0-4
        // 错误码 35 = UNSUPPORTED_VERSION
        int16_t error_code = 0;
        if (request_api_version < 0 || request_api_version > 4) 
        {
            error_code = 35;  // UNSUPPORTED_VERSION
        }
        
        // 构建响应体
        // 响应结构 (ApiVersions v4):
        //   Header: correlation_id (4字节)
        //   Body:
        //     error_code (2字节)
        //     api_keys COMPACT_ARRAY: 长度 (unsigned varint, N+1) + N 个条目
        //       每个条目: api_key (2) + min_version (2) + max_version (2) + TAG_BUFFER (1)
        //     throttle_time_ms (4字节)
        //     TAG_BUFFER (1字节)
        
        char response[256];
        int offset = 0;
        
        // 先跳过 message_size (4字节)，最后再填充
        offset = 4;
        
        // correlation_id (4字节) - 已经是网络字节序
        memcpy(response + offset, &correlation_id, 4);
        offset += 4;
        
        // error_code (2字节)
        int16_t error_code_net = htons(error_code);
        memcpy(response + offset, &error_code_net, 2);
        offset += 2;
        
        // api_keys COMPACT_ARRAY
        // COMPACT_ARRAY 长度编码为 unsigned varint，值为 N+1（N 为实际数量）
        // 我们有 1 个条目，所以长度 = 1 + 1 = 2
        uint8_t array_length = 2;  // 1 个元素 + 1
        response[offset++] = array_length;
        
        // ApiVersions 条目 (api_key = 18)
        int16_t api_key = htons(18);
        memcpy(response + offset, &api_key, 2);
        offset += 2;
        
        int16_t min_version = htons(0);
        memcpy(response + offset, &min_version, 2);
        offset += 2;
        
        int16_t max_version = htons(4);
        memcpy(response + offset, &max_version, 2);
        offset += 2;
        
        // api_key 条目的 TAG_BUFFER（空）
        response[offset++] = 0;
        
        // throttle_time_ms (4字节)
        int32_t throttle_time_ms = htonl(0);
        memcpy(response + offset, &throttle_time_ms, 4);
        offset += 4;
        
        // 响应的 TAG_BUFFER（空）
        response[offset++] = 0;
        
        // 现在填充 message_size（总大小减去 message_size 自身的 4 字节）
        // message_size = offset - 4
        int32_t message_size = htonl(offset - 4);
        memcpy(response, &message_size, 4);
        
        // 发送完整响应
        send(client_fd, response, offset, 0);
    }
    
    close(client_fd);

    close(server_fd);
    
    return 0;
}