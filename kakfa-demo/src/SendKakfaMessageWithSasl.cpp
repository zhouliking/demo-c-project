#include <rdkafka.h>
#include <iostream>
#include <cstring>
#include <cerrno>

int main(int argc, char *argv[]) {
    const char *brokers = "xx.xx.xx.xx:8092"; // Kafka broker地址
    const char *username = "xxx";
    const char *password = "xxx";
    const char *topic_name = "kafka_msg_test_sasl";
    const char *payload = "Hello, Kafka from librdkafka! sasl";
    size_t len = strlen(payload);
     // 初始化配置
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf)
    {
        std::cerr << "Failed to create configuration object: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        return 1;
    }
    char errstr[512]; // 声明一个足够大的字符数组来存储错误信息

    // 设置SASL相关的配置
    if (rd_kafka_conf_set(conf, "security.protocol", "SASL_PLAINTEXT", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        std::cerr << "Failed to set security.protocol: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
    if (rd_kafka_conf_set(conf, "sasl.mechanisms", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        std::cerr << "Failed to set sasl.mechanisms: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
    if (rd_kafka_conf_set(conf, "sasl.username", username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        std::cerr << "Failed to set sasl.username: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
    if (rd_kafka_conf_set(conf, "sasl.password", password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        std::cerr << "Failed to set sasl.password: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        std::cerr << "Failed to set bootstrap.servers: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_conf_destroy(conf);
        return 1;
    }
    // 检查配置是否设置成功
    if (rd_kafka_conf_set(conf, "security.protocol", "SASL_PLAINTEXT", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "Failed to set configuration: " << errstr << std::endl;
        return 1;
    }

    // 创建producer实例
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        std::cerr << "Failed to create new producer: " << errstr << std::endl;
        return 1;
    }

    // 创建topic句柄（可选，但推荐）
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic_name, NULL);
    if (!rkt)
    {
        std::cerr << "Failed to create topic handle: " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
        rd_kafka_destroy(rk);
        //        rd_kafka_conf_destroy(conf);
        return 1;
    }

    // 发送消息
    int32_t partition = RD_KAFKA_PARTITION_UA; // 自动选择分区
    int err = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, const_cast<char *>(payload), len, NULL, 0, NULL);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        std::cerr << "Failed to produce to topic " << topic_name << ": " << err << std::endl;
    }
    else
    {
        std::cout << "Produced " << len << " bytes to topic " << topic_name << std::endl;
    }

    // 等待所有消息发送完成（可选，但推荐）
    // 在实际生产代码中，您可能需要更复杂的逻辑来处理消息的发送和确认
    int msgs_sent = 0;
    while (rd_kafka_outq_len(rk) > 0)
    {
        rd_kafka_poll(rk, 100); // 轮询Kafka队列，直到所有消息都发送出去
        msgs_sent += rd_kafka_outq_len(rk);
    }

    // 销毁topic句柄
    rd_kafka_topic_destroy(rkt);

    // 清理资源
    rd_kafka_destroy(rk);
    return 0;
}