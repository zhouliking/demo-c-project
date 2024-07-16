#include <rdkafka.h>
#include <iostream>
#include <cstring>
#include <cerrno>
  
int main() {
    const char *brokers = "xx.xx.xx.xx:7091"; // Kafka broker地址
    const char *topic_name = "kafka_msg_topic_test";
    const char *payload = "Hello, Kafka from librdkafka!";
    size_t len = strlen(payload);
  
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
  
    // 创建生产者实例
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
    if (!rk) {
        std::cerr << "Failed to create producer: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
  
    // 创建topic句柄（可选，但推荐）
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic_name, NULL);
    if (!rkt) {
        std::cerr << "Failed to create topic handle: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_destroy(rk);
//        rd_kafka_conf_destroy(conf);
        return 1;
    }
  
    // 发送消息
    int32_t partition = RD_KAFKA_PARTITION_UA; // 自动选择分区
    int err = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, const_cast<char *>(payload), len, NULL, 0, NULL);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "Failed to produce to topic " << topic_name << ": " << err << std::endl;
    } else {
        std::cout << "Produced " << len << " bytes to topic " << topic_name << std::endl;
    }
  
    // 等待所有消息发送完成（可选，但推荐）
    // 在实际生产代码中，您可能需要更复杂的逻辑来处理消息的发送和确认
    int msgs_sent = 0;
    while (rd_kafka_outq_len(rk) > 0) {
        rd_kafka_poll(rk, 100); // 轮询Kafka队列，直到所有消息都发送出去
        msgs_sent += rd_kafka_outq_len(rk);
    }
  
    // 销毁topic句柄
    rd_kafka_topic_destroy(rkt);
  
    // 销毁生产者实例
    rd_kafka_destroy(rk);
  
    // 销毁配置对象
//    rd_kafka_conf_destroy(conf);
  
    return 0;
}

