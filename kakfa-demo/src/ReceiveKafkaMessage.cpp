#include <rdkafka.h>
#include <iostream>
#include <cerrno>
#include <cstring>
#include <cstdlib>
  
void error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    // 错误处理回调
    std::cerr << "Kafka error: " << err << ": " << reason << std::endl;
}
  
int main() {
    std::cerr << "start " << std::endl;
    const char *brokers = "10.13.1.11:7091"; // Kafka broker地址
    const char *group_id = "dashi_kafka_msg_yefeilang_group_test"; // 消费者组ID
    const char *topic_name = "kafka_msg_topic_test"; // Kafka topic名称
    
    // 创建配置对象
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf) {
        std::cerr << "Failed to create configuration object: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        return 1;
    }
    
    // 设置broker地址
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, NULL, 0) != RD_KAFKA_CONF_OK) {
        std::cerr << "Failed to set bootstrap.servers: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
    
    // 设置消费者组ID
    if (rd_kafka_conf_set(conf, "group.id", group_id, NULL, 0) != RD_KAFKA_CONF_OK) {
        std::cerr << "Failed to set group.id: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
    
    // 设置错误处理回调（可选）
    rd_kafka_conf_set_error_cb(conf, error_cb);
    
    // 创建消费者实例
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, NULL, 0);
    if (!rk) {
        std::cerr << "Failed to create consumer: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        return 1;
    }
    
    
    // 创建一个topic分区列表
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    if (!topics) {
        std::cerr << "Failed to create topic partition list: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_destroy(rk);
        return 1;
    }
    
    // 添加topic到分区列表
    if (!rd_kafka_topic_partition_list_add(topics, topic_name, RD_KAFKA_PARTITION_UA)) {
        std::cerr << "Failed to add topic to partition list: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_topic_partition_list_destroy(topics);
        rd_kafka_destroy(rk);
        return 1;
    }
    // 订阅topic
    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe to topic: " << rd_kafka_err2str(err) << std::endl;
        rd_kafka_topic_partition_list_destroy(topics);
        rd_kafka_destroy(rk);
        return 1;
    }
    
    // 销毁分区列表（订阅后不再需要）
    rd_kafka_topic_partition_list_destroy(topics);
    
    
    // 轮询消息
    while (true) {
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk, 1000); // 等待1秒以获取消息
        
        if (rkmessage == NULL) {
            // 没有消息或者超时
            continue;
        }
        
        if (rkmessage->err) {
            // 处理错误
            if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                // 消息流的末尾
                std::cout << "End of partition event" << std::endl;
            } else {
                // 打印错误并退出
                std::cerr << "Kafka consumer error: " << rd_kafka_message_errstr(rkmessage) << std::endl;
                break;
            }
        } else {
            // 处理消息
            std::cout << "Received message at offset " << rkmessage->offset
            << " from partition " << rkmessage->partition
            << " with key \"" << rkmessage->key << "\" and payload size "<< rkmessage->len
            << " value :" <<(char *)rkmessage->payload
            << std::endl;
            
            // 如果需要，可以在这里处理消息内容
            // 例如，使用rkmessage->payload()获取消息内容
            
            // 释放消息
            rd_kafka_message_destroy(rkmessage);
        }
    }
    
    // 清理
    rd_kafka_destroy(rk);
    
    return 0;
}