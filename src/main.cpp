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
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>

// Topic 信息结构
struct TopicInfo
{
    std::string name;
    char uuid[16];
    bool found = false;
};

// Partition 信息结构
struct PartitionInfo
{
    int32_t partition_index;
    int32_t leader_id;
    int32_t leader_epoch;
    std::vector<int32_t> replica_nodes;
};

// 全局存储 topic 和 partition 信息
std::map<std::string, TopicInfo> g_topics;                    // topic_name -> TopicInfo
std::map<std::string, std::vector<PartitionInfo>> g_partitions;  // topic_uuid_hex -> partitions

// 读取 varint (有符号 zigzag 编码)
int64_t read_varint(const char* data, int& offset)
{
    int64_t result = 0;
    int shift = 0;
    while (true)
    {
        uint8_t byte = static_cast<uint8_t>(data[offset++]);
        result |= static_cast<int64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0)
        {
            break;
        }
        shift += 7;
    }
    // zigzag 解码
    return (result >> 1) ^ -(result & 1);
}

// 读取 unsigned varint
uint64_t read_unsigned_varint(const char* data, int& offset)
{
    uint64_t result = 0;
    int shift = 0;
    while (true)
    {
        uint8_t byte = static_cast<uint8_t>(data[offset++]);
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0)
        {
            break;
        }
        shift += 7;
    }
    return result;
}

// UUID 转换为十六进制字符串
std::string uuid_to_hex(const char* uuid)
{
    char hex[33];
    for (int i = 0; i < 16; i++)
    {
        sprintf(hex + i * 2, "%02x", static_cast<uint8_t>(uuid[i]));
    }
    hex[32] = '\0';
    return std::string(hex);
}

// 解析集群元数据日志文件
void parse_cluster_metadata(const std::string& log_path)
{
    std::ifstream file(log_path, std::ios::binary);
    if (!file.is_open())
    {
        std::cerr << "Failed to open cluster metadata log: " << log_path << std::endl;
        return;
    }
    
    // 读取整个文件
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<char> data(file_size);
    file.read(data.data(), file_size);
    file.close();
    
    size_t pos = 0;
    while (pos + 61 < file_size)  // RecordBatch 最小头部大小
    {
        // 读取 RecordBatch 头部
        // baseOffset (8) + batchLength (4) + ...
        int64_t baseOffset;
        memcpy(&baseOffset, data.data() + pos, 8);
        baseOffset = __builtin_bswap64(baseOffset);
        
        int32_t batchLength;
        memcpy(&batchLength, data.data() + pos + 8, 4);
        batchLength = __builtin_bswap32(batchLength);
        
        if (batchLength <= 0 || pos + 12 + batchLength > file_size)
        {
            break;
        }
        
        // 跳到 records 部分
        // RecordBatch header: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) 
        //                    + attributes(2) + lastOffsetDelta(4) + baseTimestamp(8) + maxTimestamp(8)
        //                    + producerId(8) + producerEpoch(2) + baseSequence(4) + recordCount(4)
        // Total: 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 = 61
        
        int32_t recordCount;
        memcpy(&recordCount, data.data() + pos + 57, 4);
        recordCount = __builtin_bswap32(recordCount);
        
        int record_offset = pos + 61;
        
        for (int i = 0; i < recordCount && record_offset < pos + 12 + batchLength; i++)
        {
            // 读取 Record
            int rec_start = record_offset;
            int64_t length = read_varint(data.data(), record_offset);
            int rec_end = rec_start + length + (record_offset - rec_start);
            
            // attributes (1 byte)
            record_offset += 1;
            
            // timestampDelta (varint)
            read_varint(data.data(), record_offset);
            
            // offsetDelta (varint)
            read_varint(data.data(), record_offset);
            
            // keyLength (varint)
            int64_t keyLength = read_varint(data.data(), record_offset);
            if (keyLength > 0)
            {
                record_offset += keyLength;
            }
            
            // valueLength (varint)
            int64_t valueLength = read_varint(data.data(), record_offset);
            
            if (valueLength > 0)
            {
                // 解析 value（cluster metadata record）
                int value_start = record_offset;
                
                // frame version (1 byte) + record type (1 byte) + version (1 byte)
                uint8_t frame_version = static_cast<uint8_t>(data[record_offset]);
                uint8_t record_type = static_cast<uint8_t>(data[record_offset + 1]);
                
                if (record_type == 2)  // TopicRecord
                {
                    // TopicRecord: frame_version(1) + type(1) + version(1) + name_len(varint) + name + uuid(16) + TAG_BUFFER
                    int val_offset = value_start + 2;  // skip frame_version and type
                    uint8_t version = static_cast<uint8_t>(data[val_offset]);
                    val_offset += 1;
                    
                    // topic name (COMPACT_STRING)
                    int64_t name_len = read_unsigned_varint(data.data(), val_offset) - 1;
                    std::string topic_name(data.data() + val_offset, name_len);
                    val_offset += name_len;
                    
                    // topic uuid (16 bytes)
                    TopicInfo info;
                    info.name = topic_name;
                    memcpy(info.uuid, data.data() + val_offset, 16);
                    info.found = true;
                    
                    g_topics[topic_name] = info;
                }
                else if (record_type == 3)  // PartitionRecord
                {
                    // PartitionRecord: frame_version(1) + type(1) + version(1) + partition_id(4) + topic_uuid(16) + ...
                    int val_offset = value_start + 2;  // skip frame_version and type
                    uint8_t version = static_cast<uint8_t>(data[val_offset]);
                    val_offset += 1;
                    
                    // partition_id (4 bytes)
                    int32_t partition_id;
                    memcpy(&partition_id, data.data() + val_offset, 4);
                    partition_id = __builtin_bswap32(partition_id);
                    val_offset += 4;
                    
                    // topic_uuid (16 bytes)
                    char topic_uuid[16];
                    memcpy(topic_uuid, data.data() + val_offset, 16);
                    val_offset += 16;
                    
                    // replicas COMPACT_ARRAY
                    uint64_t replicas_len = read_unsigned_varint(data.data(), val_offset) - 1;
                    std::vector<int32_t> replicas;
                    for (uint64_t r = 0; r < replicas_len; r++)
                    {
                        int32_t replica;
                        memcpy(&replica, data.data() + val_offset, 4);
                        replica = __builtin_bswap32(replica);
                        replicas.push_back(replica);
                        val_offset += 4;
                    }
                    
                    // isr COMPACT_ARRAY
                    uint64_t isr_len = read_unsigned_varint(data.data(), val_offset) - 1;
                    val_offset += isr_len * 4;  // skip isr
                    
                    // removing_replicas COMPACT_ARRAY
                    uint64_t removing_len = read_unsigned_varint(data.data(), val_offset) - 1;
                    val_offset += removing_len * 4;
                    
                    // adding_replicas COMPACT_ARRAY
                    uint64_t adding_len = read_unsigned_varint(data.data(), val_offset) - 1;
                    val_offset += adding_len * 4;
                    
                    // leader (4 bytes)
                    int32_t leader;
                    memcpy(&leader, data.data() + val_offset, 4);
                    leader = __builtin_bswap32(leader);
                    val_offset += 4;
                    
                    // leader_epoch (4 bytes)
                    int32_t leader_epoch;
                    memcpy(&leader_epoch, data.data() + val_offset, 4);
                    leader_epoch = __builtin_bswap32(leader_epoch);
                    
                    PartitionInfo pinfo;
                    pinfo.partition_index = partition_id;
                    pinfo.leader_id = leader;
                    pinfo.leader_epoch = leader_epoch;
                    pinfo.replica_nodes = replicas;
                    
                    std::string uuid_hex = uuid_to_hex(topic_uuid);
                    g_partitions[uuid_hex].push_back(pinfo);
                }
                
                record_offset = value_start + valueLength;
            }
            
            // headers count (varint)
            read_unsigned_varint(data.data(), record_offset);
            
            // 移动到下一个 record
            record_offset = rec_end;
        }
        
        // 移动到下一个 batch
        pos += 12 + batchLength;
    }
}

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
    // 我们有 3 个条目，所以长度 = 3 + 1 = 4
    uint8_t array_length = 4;  // 3 个元素 + 1
    response[offset++] = array_length;
    
    // Fetch 条目 (api_key = 1)
    int16_t api_key_fetch = htons(1);
    memcpy(response + offset, &api_key_fetch, 2);
    offset += 2;
    
    int16_t min_version_fetch = htons(0);
    memcpy(response + offset, &min_version_fetch, 2);
    offset += 2;
    
    int16_t max_version_fetch = htons(16);
    memcpy(response + offset, &max_version_fetch, 2);
    offset += 2;
    
    // api_key 条目的 TAG_BUFFER（空）
    response[offset++] = 0;
    
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
    // 解析请求体，获取多个 topic_name
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
    
    // 读取所有请求的 topic names
    std::vector<std::string> requested_topics;
    for (int i = 0; i < num_topics; i++)
    {
        // 读取 topic name (COMPACT_STRING)
        uint8_t topic_name_len_encoded = static_cast<uint8_t>(buffer[req_offset]);
        req_offset += 1;
        int topic_name_len = topic_name_len_encoded - 1;  // 实际长度
        
        std::string topic_name(buffer + req_offset, topic_name_len);
        requested_topics.push_back(topic_name);
        req_offset += topic_name_len;
        
        // 跳过 topic 条目的 TAG_BUFFER
        req_offset += 1;
    }
    
    // 按字母顺序排序
    std::sort(requested_topics.begin(), requested_topics.end());
    
    // 构建响应
    char response[4096];  // 增大缓冲区以支持多个 topic
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
    
    // topics COMPACT_ARRAY: N 个元素，长度 = N + 1
    response[offset++] = static_cast<uint8_t>(num_topics + 1);
    
    // 为每个 topic 构建响应条目
    for (const auto& topic_name : requested_topics)
    {
        // 查找 topic 是否存在
        bool topic_exists = false;
        TopicInfo* topic_info = nullptr;
        std::vector<PartitionInfo>* partitions = nullptr;
        
        auto it = g_topics.find(topic_name);
        if (it != g_topics.end() && it->second.found)
        {
            topic_exists = true;
            topic_info = &it->second;
            std::string uuid_hex = uuid_to_hex(topic_info->uuid);
            auto pit = g_partitions.find(uuid_hex);
            if (pit != g_partitions.end())
            {
                partitions = &pit->second;
            }
        }
        
        if (topic_exists && topic_info)
        {
            // Topic 存在
            // error_code (2字节) - 0 = NO_ERROR
            int16_t error_code = htons(0);
            memcpy(response + offset, &error_code, 2);
            offset += 2;
            
            // topic_name COMPACT_STRING
            response[offset++] = static_cast<uint8_t>(topic_name.size() + 1);
            memcpy(response + offset, topic_name.c_str(), topic_name.size());
            offset += topic_name.size();
            
            // topic_id UUID (16字节) - 从元数据获取
            memcpy(response + offset, topic_info->uuid, 16);
            offset += 16;
            
            // is_internal BOOLEAN (1字节) - false
            response[offset++] = 0;
            
            // partitions COMPACT_ARRAY
            if (partitions && !partitions->empty())
            {
                response[offset++] = static_cast<uint8_t>(partitions->size() + 1);
                
                for (const auto& part : *partitions)
                {
                    // error_code (2字节) - 0
                    int16_t part_error = htons(0);
                    memcpy(response + offset, &part_error, 2);
                    offset += 2;
                    
                    // partition_index (4字节)
                    int32_t part_idx = htonl(part.partition_index);
                    memcpy(response + offset, &part_idx, 4);
                    offset += 4;
                    
                    // leader_id (4字节)
                    int32_t leader = htonl(part.leader_id);
                    memcpy(response + offset, &leader, 4);
                    offset += 4;
                    
                    // leader_epoch (4字节)
                    int32_t epoch = htonl(part.leader_epoch);
                    memcpy(response + offset, &epoch, 4);
                    offset += 4;
                    
                    // replica_nodes COMPACT_ARRAY
                    response[offset++] = static_cast<uint8_t>(part.replica_nodes.size() + 1);
                    for (int32_t replica : part.replica_nodes)
                    {
                        int32_t rep = htonl(replica);
                        memcpy(response + offset, &rep, 4);
                        offset += 4;
                    }
                    
                    // isr_nodes COMPACT_ARRAY (same as replicas for simplicity)
                    response[offset++] = static_cast<uint8_t>(part.replica_nodes.size() + 1);
                    for (int32_t replica : part.replica_nodes)
                    {
                        int32_t rep = htonl(replica);
                        memcpy(response + offset, &rep, 4);
                        offset += 4;
                    }
                    
                    // eligible_leader_replicas COMPACT_ARRAY - 空
                    response[offset++] = 1;
                    
                    // last_known_elr COMPACT_ARRAY - 空
                    response[offset++] = 1;
                    
                    // offline_replicas COMPACT_ARRAY - 空
                    response[offset++] = 1;
                    
                    // TAG_BUFFER（空）- partition 条目
                    response[offset++] = 0;
                }
            }
            else
            {
                // 没有 partition 信息
                response[offset++] = 1;  // 空数组
            }
        }
        else
        {
            // Topic 不存在
            // error_code (2字节) - 3 = UNKNOWN_TOPIC_OR_PARTITION
            int16_t error_code = htons(3);
            memcpy(response + offset, &error_code, 2);
            offset += 2;
            
            // topic_name COMPACT_STRING
            response[offset++] = static_cast<uint8_t>(topic_name.size() + 1);
            memcpy(response + offset, topic_name.c_str(), topic_name.size());
            offset += topic_name.size();
            
            // topic_id UUID (16字节) - 全零
            memset(response + offset, 0, 16);
            offset += 16;
            
            // is_internal BOOLEAN (1字节) - false
            response[offset++] = 0;
            
            // partitions COMPACT_ARRAY - 空数组
            response[offset++] = 1;
        }
        
        // topic_authorized_operations INT32 (4字节) - 0
        int32_t auth_ops = htonl(0);
        memcpy(response + offset, &auth_ops, 4);
        offset += 4;
        
        // TAG_BUFFER（空）- topic 条目
        response[offset++] = 0;
    }
    
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

// 处理 Fetch 请求
void handle_fetch(int client_fd, int32_t correlation_id, char* buffer, ssize_t bytes_read)
{
    // Fetch Request v16 结构:
    //   Request Header v2:
    //     message_size (4) + api_key (2) + api_version (2) + correlation_id (4)
    //     + client_id (NULLABLE_STRING: 2字节长度 + 内容) + TAG_BUFFER
    //   Request Body:
    //     max_wait_ms (4) + min_bytes (4) + max_bytes (4) + isolation_level (1)
    //     + session_id (4) + session_epoch (4) + topics COMPACT_ARRAY + ...
    
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
    // 跳过: max_wait_ms (4) + min_bytes (4) + max_bytes (4) + isolation_level (1)
    //       + session_id (4) + session_epoch (4) = 21 字节
    req_offset += 21;
    
    // topics COMPACT_ARRAY: 长度 (N+1)
    uint8_t topics_array_len = static_cast<uint8_t>(buffer[req_offset]);
    req_offset += 1;
    int num_topics = topics_array_len - 1;
    
    // 读取所有请求的 topic_ids 和 partition 信息
    struct FetchPartitionRequest
    {
        int32_t partition_index;
    };
    struct FetchTopicRequest
    {
        char topic_id[16];
        std::vector<FetchPartitionRequest> partitions;
    };
    std::vector<FetchTopicRequest> requested_topics;
    
    for (int i = 0; i < num_topics; i++)
    {
        FetchTopicRequest topic_req;
        // topic_id UUID (16 字节)
        memcpy(topic_req.topic_id, buffer + req_offset, 16);
        req_offset += 16;
        
        // partitions COMPACT_ARRAY
        uint8_t partitions_len = static_cast<uint8_t>(buffer[req_offset]);
        req_offset += 1;
        int num_partitions = partitions_len - 1;
        
        for (int p = 0; p < num_partitions; p++)
        {
            FetchPartitionRequest part_req;
            // partition (4 字节)
            int32_t part_idx_net;
            memcpy(&part_idx_net, buffer + req_offset, 4);
            part_req.partition_index = ntohl(part_idx_net);
            req_offset += 4;
            
            // 跳过: current_leader_epoch (4) + fetch_offset (8) + last_fetched_epoch (4)
            //       + log_start_offset (8) + partition_max_bytes (4) + TAG_BUFFER (1) = 29 字节
            req_offset += 29;
            
            topic_req.partitions.push_back(part_req);
        }
        
        // 跳过 topic 的 TAG_BUFFER
        req_offset += 1;
        
        requested_topics.push_back(topic_req);
    }
    
    // 构建响应 - 使用动态缓冲区
    std::vector<char> response(65536);
    int offset = 0;
    
    // 先跳过 message_size (4字节)，最后再填充
    offset = 4;
    
    // Response Header v1
    // correlation_id (4字节) - 已经是网络字节序
    memcpy(response.data() + offset, &correlation_id, 4);
    offset += 4;
    
    // TAG_BUFFER（空）- response header v1 特有
    response[offset++] = 0;
    
    // Response Body
    // throttle_time_ms (4字节)
    int32_t throttle_time_ms = htonl(0);
    memcpy(response.data() + offset, &throttle_time_ms, 4);
    offset += 4;
    
    // error_code (2字节) - 0 = NO_ERROR
    int16_t error_code = htons(0);
    memcpy(response.data() + offset, &error_code, 2);
    offset += 2;
    
    // session_id (4字节) - 0
    int32_t session_id = htonl(0);
    memcpy(response.data() + offset, &session_id, 4);
    offset += 4;
    
    // responses COMPACT_ARRAY
    response[offset++] = static_cast<uint8_t>(num_topics + 1);
    
    for (const auto& topic_req : requested_topics)
    {
        // 检查 topic_id 是否存在，并获取 topic_name
        std::string topic_uuid_hex = uuid_to_hex(topic_req.topic_id);
        bool topic_exists = false;
        std::string topic_name;
        
        // 在 g_topics 中查找匹配的 topic_id
        for (const auto& topic_pair : g_topics)
        {
            std::string stored_uuid_hex = uuid_to_hex(topic_pair.second.uuid);
            if (stored_uuid_hex == topic_uuid_hex)
            {
                topic_exists = true;
                topic_name = topic_pair.first;
                break;
            }
        }
        
        // topic_id UUID (16字节)
        memcpy(response.data() + offset, topic_req.topic_id, 16);
        offset += 16;
        
        // partitions COMPACT_ARRAY
        response[offset++] = static_cast<uint8_t>(topic_req.partitions.size() + 1);
        
        for (const auto& part_req : topic_req.partitions)
        {
            // partition_index (4字节)
            int32_t partition_index = htonl(part_req.partition_index);
            memcpy(response.data() + offset, &partition_index, 4);
            offset += 4;
            
            // 读取日志文件（如果 topic 存在）
            std::vector<char> records_data;
            if (topic_exists)
            {
                // 构建日志文件路径: /tmp/kraft-combined-logs/{topic_name}-{partition}/00000000000000000000.log
                std::string log_path = "/tmp/kraft-combined-logs/" + topic_name + "-" + 
                                       std::to_string(part_req.partition_index) + "/00000000000000000000.log";
                
                std::ifstream log_file(log_path, std::ios::binary);
                if (log_file.is_open())
                {
                    log_file.seekg(0, std::ios::end);
                    size_t file_size = log_file.tellg();
                    log_file.seekg(0, std::ios::beg);
                    
                    if (file_size > 0)
                    {
                        records_data.resize(file_size);
                        log_file.read(records_data.data(), file_size);
                    }
                    log_file.close();
                }
            }
            
            // error_code (2字节) - 0 = NO_ERROR 或 100 = UNKNOWN_TOPIC_ID
            int16_t part_error = htons(topic_exists ? 0 : 100);
            memcpy(response.data() + offset, &part_error, 2);
            offset += 2;
            
            // high_watermark (8字节) - 0
            int64_t high_watermark = 0;
            memcpy(response.data() + offset, &high_watermark, 8);
            offset += 8;
            
            // last_stable_offset (8字节) - 0
            int64_t last_stable_offset = 0;
            memcpy(response.data() + offset, &last_stable_offset, 8);
            offset += 8;
            
            // log_start_offset (8字节) - 0
            int64_t log_start_offset = 0;
            memcpy(response.data() + offset, &log_start_offset, 8);
            offset += 8;
            
            // aborted_transactions COMPACT_ARRAY - 空数组
            response[offset++] = 1;
            
            // preferred_read_replica (4字节) - -1
            int32_t preferred_read_replica = htonl(-1);
            memcpy(response.data() + offset, &preferred_read_replica, 4);
            offset += 4;
            
            // records COMPACT_NULLABLE_BYTES
            if (records_data.empty())
            {
                // null 或空记录
                response[offset++] = 0;
            }
            else
            {
                // 写入 records 长度 (unsigned varint, N+1)
                size_t records_len = records_data.size();
                size_t len_to_write = records_len + 1;  // COMPACT encoding: length + 1
                
                // 编码 unsigned varint
                while (len_to_write >= 0x80)
                {
                    response[offset++] = static_cast<char>((len_to_write & 0x7F) | 0x80);
                    len_to_write >>= 7;
                }
                response[offset++] = static_cast<char>(len_to_write);
                
                // 确保缓冲区足够大
                if (offset + records_data.size() > response.size())
                {
                    response.resize(offset + records_data.size() + 1024);
                }
                
                // 写入 records 数据
                memcpy(response.data() + offset, records_data.data(), records_data.size());
                offset += records_data.size();
            }
            
            // TAG_BUFFER（空）- partition 条目
            response[offset++] = 0;
        }
        
        // TAG_BUFFER（空）- topic 条目
        response[offset++] = 0;
    }
    
    // TAG_BUFFER（空）- response body
    response[offset++] = 0;
    
    // 现在填充 message_size
    int32_t message_size = htonl(offset - 4);
    memcpy(response.data(), &message_size, 4);
    
    // 发送完整响应
    send(client_fd, response.data(), offset, 0);
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
        if (request_api_key == 1)
        {
            // Fetch
            handle_fetch(client_fd, correlation_id, buffer, bytes_read);
        }
        else if (request_api_key == 18)
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
    
    // 解析集群元数据日志文件
    std::string log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    parse_cluster_metadata(log_path);

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