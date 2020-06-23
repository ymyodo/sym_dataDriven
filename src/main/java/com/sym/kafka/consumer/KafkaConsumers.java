package com.sym.kafka.consumer;

import com.sym.kafka.constant.KafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @author shenyanming
 * @date 2020/6/6 21:47.
 */
public class KafkaConsumers {

    public static Properties initProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstant.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_DESERIALIZER);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaConstant.AUTO_COMMIT_FALSE);
        return props;
    }

    /**
     * 默认配置的kafka consumer
     */
    public static <K, V> KafkaConsumer<K, V> createConsumer() {
        // 创建消费者
        return createConsumer(initProperties());
    }

    /**
     * 创建一个kafka consumer, 它是线程安全的
     */
    public static <K, V> KafkaConsumer<K, V> createConsumer(Properties props) {
        return new KafkaConsumer<>(props);
    }

    public static Properties mergeProps(Properties... props) {
        Properties prop = new Properties();
        for (Properties p : props) {
            p.forEach(prop::put);
        }
        return prop;
    }
}
