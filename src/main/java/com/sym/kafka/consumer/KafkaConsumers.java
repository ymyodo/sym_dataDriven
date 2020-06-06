package com.sym.kafka.consumer;

import com.sym.kafka.constant.KafkaConstant;
import com.sym.util.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author shenyanming
 * @date 2020/6/6 21:47.
 */
@Slf4j
public class KafkaConsumers {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstant.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_DESERIALIZER);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConstant.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstant.OFFSET_RESET_EARLIER);

        // 创建消费者
        KafkaConsumer<Object, Object> consumer = createConsumer(props);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(KafkaConstant.TOPIC_NAME));

        // 不断地接受消息
        for (; ; ) {
            // 消费者会在这边阻塞住, 指定最大阻塞时间
            ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(1000));
            if(!records.isEmpty()){
                log.info("kafka响应：{}", JSONUtil.toJson(records));
            }
            records.forEach(record -> {
                log.info("收到消息：{}", record.value());
            });
        }
    }


    public static <K, V> KafkaConsumer<K, V> createConsumer(Properties props) {
        return new KafkaConsumer<>(props);
    }
}
