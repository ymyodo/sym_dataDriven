package com.sym.kafka.constant;

import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 创建kafka生产者{@link org.apache.kafka.clients.producer.KafkaProducer}
 * 的配置信息
 *
 * @author shenyanming
 * @date 2020/6/6 21:28.
 */
public class KafkaConstant {
    // 指定broker的地址
    public static String KAFKA_BROKERS = "localhost:9092";

    // 消费组
    public static String GROUP_ID_CONFIG = "default";

    // 字符串序列化器和字符串反序列化器
    public static String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    // 默认的topic
    public static String TOPIC_NAME = "demo";

    // 不自动提交
    public static String AUTO_COMMIT_FALSE = "false";
    public static String AUTO_COMMIT_TRUE= "true";

    public static Integer MESSAGE_COUNT = 1000;
    public static Integer RETRIES_COUNT = 2;
    public static String CLIENT_ID = "client1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;

}
