package com.sym.kafka.producer;

import com.sym.kafka.constant.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author shenyanming
 * @date 2020/6/6 21:36.
 */
@Slf4j
public class KafkaProducers {

    /**
     * 创建一个默认配置的kafka producer
     */
    public static <K, V> KafkaProducer<K, V> createProducer() {
        // kafka producer 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstant.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_SERIALIZER);
        props.put(ProducerConfig.RETRIES_CONFIG, KafkaConstant.RETRIES_COUNT);
        return createProducer(props);
    }

    /**
     * 创建一个kafka producer
     */
    public static <K, V> KafkaProducer<K, V> createProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    /**
     * 创建消息体
     */
    public static <K, V> ProducerRecord<K, V> createRecord(String topic, V value) {
        return new ProducerRecord<>(topic, value);
    }
}
