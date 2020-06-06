package com.sym.kafka.producer;

import com.sym.kafka.constant.KafkaConstant;
import com.sym.util.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author shenyanming
 * @date 2020/6/6 21:36.
 */
@Slf4j
public class KafkaProducers {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // kafka producer 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstant.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  KafkaConstant.STRING_SERIALIZER);
        props.put(ProducerConfig.RETRIES_CONFIG,  KafkaConstant.RETRIES_COUNT);

        // 创建生产者
        KafkaProducer<String, String> producer = createProducer(props);

        // 创建消息体
        ProducerRecord<String, String> record = createRecord(KafkaConstant.TOPIC_NAME, "yeah!kafka");

        // 发送消息
        Future<RecordMetadata> future = producer.send(record);

        // 获取响应
        RecordMetadata metadata = future.get();
        log.info("响应：{}", JSONUtil.toJson(metadata));
    }

    /**
     * 创建一个kafka producer
     */
    public static <K, V> KafkaProducer<K, V> createProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    public static <K,V>ProducerRecord<K,V> createRecord(String topic, V value){
        return new ProducerRecord<>(topic, value);
    }
}
