#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>

// 处理 ApiVersions 请求
void handle_api_versions(int client_fd, int32_t correlation_id, int16_t request_api_version)
{
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
    //   Header v0: correlation_id (4字节)
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
    // 我们有 2 个条目，所以长度 = 2 + 1 = 3
    uint8_t array_length = 3;  // 2 个元素 + 1
    response[offset++] = array_length;
    
    // ApiVersions 条目 (api_key = 18)
    int16_t api_key_1 = htons(18);
    memcpy(response + offset, &api_key_1, 2);
    offset += 2;
    
    int16_t min_version_1 = htons(0);
    memcpy(response + offset, &min_version_1, 2);
    offset += 2;
    
    int16_t max_version_1 = htons(4);
    memcpy(response + offset, &max_version_1, 2);
    offset += 2;
    
    // api_key 条目的 TAG_BUFFER（空）
    response[offset++] = 0;
    
    // DescribeTopicPartitions 条目 (api_key = 75)
    int16_t api_key_2 = htons(75);
    memcpy(response + offset, &api_key_2, 2);
    offset += 2;
    
    int16_t min_version_2 = htons(0);
    memcpy(response + offset, &min_version_2, 2);
    offset += 2;
    
    int16_t max_version_2 = htons(0);
    memcpy(response + offset, &max_version_2, 2);
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
    int32_t message_size = htonl(offset - 4);
    memcpy(response, &message_size, 4);
    
    // 发送完整响应
    send(client_fd, response, offset, 0);
}

// 处理 DescribeTopicPartitions 请求
void handle_describe_topic_partitions(int client_fd, int32_t correlation_id, char* buffer, ssize_t bytes_read)
{
    // 解析请求体，获取 topic_name
    // 请求头 v2 结构:
    //   message_size (4) + api_key (2) + api_version (2) + correlation_id (4) 
    //   + client_id (NULLABLE_STRING: 2字节长度 + 内容) + TAG_BUFFER
    // 请求体:
    //   topics COMPACT_ARRAY + ...
    
    // 跳过请求头固定部分: 4 + 2 + 2 + 4 = 12
    int req_offset = 12;
    
    // 跳过 client_id (NULLABLE_STRING: 2字节长度前缀)
    int16_t client_id_len_net;
    memcpy(&client_id_len_net, buffer + req_offset, 2);
    int16_t client_id_len = ntohs(client_id_len_net);
    req_offset += 2;
    if (client_id_len > 0)
    {
        req_offset += client_id_len;
    }
    
    // 跳过请求头的 TAG_BUFFER
    req_offset += 1;
    
    // 现在是请求体
    // topics COMPACT_ARRAY: 长度 (N+1)
    uint8_t topics_array_len = static_cast<uint8_t>(buffer[req_offset]);
    req_offset += 1;
    int num_topics = topics_array_len - 1;
    
    // 读取第一个 topic 的 name (COMPACT_STRING)
    uint8_t topic_name_len_encoded = static_cast<uint8_t>(buffer[req_offset]);
    req_offset += 1;
    int topic_name_len = topic_name_len_encoded - 1;  // 实际长度
    
    std::string topic_name(buffer + req_offset, topic_name_len);
    
    // 构建响应
    // 响应结构 (DescribeTopicPartitions v0):
    //   Header v1: correlation_id (4) + TAG_BUFFER (1)
    //   Body:
    //     throttle_time_ms (4)
    //     topics COMPACT_ARRAY
    //       error_code (2)
    //       topic_name COMPACT_STRING
    //       topic_id UUID (16)
    //       is_internal BOOLEAN (1)
    //       partitions COMPACT_ARRAY (empty)
    //       topic_authorized_operations INT32 (4)
    //       TAG_BUFFER (1)
    //     next_cursor NULLABLE_INT8 (1)
    //     TAG_BUFFER (1)
    
    char response[256];
    int offset = 0;
    
    // 先跳过 message_size (4字节)，最后再填充
    offset = 4;
    
    // Response Header v1
    // correlation_id (4字节) - 已经是网络字节序
    memcpy(response + offset, &correlation_id, 4);
    offset += 4;
    
    // TAG_BUFFER（空）- response header v1 特有
    response[offset++] = 0;
    
    // Response Body
    // throttle_time_ms (4字节)
    int32_t throttle_time_ms = htonl(0);
    memcpy(response + offset, &throttle_time_ms, 4);
    offset += 4;
    
    // topics COMPACT_ARRAY: 1 个元素，长度 = 1 + 1 = 2
    response[offset++] = 2;
    
    // error_code (2字节) - 3 = UNKNOWN_TOPIC_OR_PARTITION
    int16_t error_code = htons(3);
    memcpy(response + offset, &error_code, 2);
    offset += 2;
    
    // topic_name COMPACT_STRING: 长度 = actual_len + 1
    response[offset++] = static_cast<uint8_t>(topic_name_len + 1);
    memcpy(response + offset, topic_name.c_str(), topic_name_len);
    offset += topic_name_len;
    
    // topic_id UUID (16字节) - 全零
    memset(response + offset, 0, 16);
    offset += 16;
    
    // is_internal BOOLEAN (1字节) - false
    response[offset++] = 0;
    
    // partitions COMPACT_ARRAY - 空数组，长度 = 0 + 1 = 1
    response[offset++] = 1;
    
    // topic_authorized_operations INT32 (4字节) - 0
    int32_t auth_ops = htonl(0);
    memcpy(response + offset, &auth_ops, 4);
    offset += 4;
    
    // TAG_BUFFER（空）- topic 条目
    response[offset++] = 0;
    
    // next_cursor NULLABLE_INT8 - -1 表示 null
    response[offset++] = 0xff;
    
    // TAG_BUFFER（空）- response body
    response[offset++] = 0;
    
    // 现在填充 message_size
    int32_t message_size = htonl(offset - 4);
    memcpy(response, &message_size, 4);
    
    // 发送完整响应
    send(client_fd, response, offset, 0);
}

// 处理单个客户端连接的函数
void handle_client(int client_fd)
{
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
        
        // request_api_key 在偏移量 4 处
        int16_t request_api_key_net;
        memcpy(&request_api_key_net, buffer + 4, sizeof(request_api_key_net));
        int16_t request_api_key = ntohs(request_api_key_net);
        
        // request_api_version 在偏移量 6 处 (4 + 2)
        int16_t request_api_version_net;
        memcpy(&request_api_version_net, buffer + 6, sizeof(request_api_version_net));
        int16_t request_api_version = ntohs(request_api_version_net);
        
        // correlation_id 在偏移量 8 处 (4 + 2 + 2)
        int32_t correlation_id;
        memcpy(&correlation_id, buffer + 8, sizeof(correlation_id));
        // correlation_id 已经是网络字节序（大端），回显时无需转换
        
        // 根据 api_key 分发处理
        if (request_api_key == 18)
        {
            // ApiVersions
            handle_api_versions(client_fd, correlation_id, request_api_version);
        }
        else if (request_api_key == 75)
        {
            // DescribeTopicPartitions
            handle_describe_topic_partitions(client_fd, correlation_id, buffer, bytes_read);
        }
    }
    
    close(client_fd);
}

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

    // 调试信息会在运行测试时显示
    std::cerr << "Logs from your program will appear here!\n";
    
    // 主循环：接受多个客户端连接
    while (true)
    {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
        if (client_fd < 0)
        {
            std::cerr << "accept failed" << std::endl;
            continue;
        }
        
        std::cout << "Client connected\n";
        
        // 为每个客户端创建一个新线程处理
        std::thread client_thread(handle_client, client_fd);
        client_thread.detach();  // 分离线程，让它独立运行
    }

    close(server_fd);
    
    return 0;
}