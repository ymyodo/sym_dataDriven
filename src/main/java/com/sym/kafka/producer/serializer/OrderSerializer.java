package com.sym.kafka.producer.serializer;

import com.sym.kafka.constant.KafkaConstant;
import com.sym.kafka.domain.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Properties;

/**
 * 自定义序列化器{@link Serializer}
 *
 * @author shenyanming
 * @date 2020/6/14 20:54.
 */
public class OrderSerializer implements Serializer<Order> {

    public static Properties properties(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstant.STRING_SERIALIZER);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.sym.kafka.producer.serializer.OrderSerializer");
        return prop;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Order data) {
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Order data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
