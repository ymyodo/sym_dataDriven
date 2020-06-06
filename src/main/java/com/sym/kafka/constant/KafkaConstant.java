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
    public static String KAFKA_BROKERS = "localhost:9092";
    public static Integer MESSAGE_COUNT = 1000;
    public static Integer RETRIES_COUNT = 2;
    public static String CLIENT_ID = "client1";
    public static String TOPIC_NAME = "demo";
    public static String GROUP_ID_CONFIG = "consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;
    public static String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
}
