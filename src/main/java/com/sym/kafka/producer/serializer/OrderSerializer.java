package com.sym.kafka.producer.serializer;

import com.sym.kafka.domain.Order;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 自定义序列化器{@link Serializer}
 *
 * @author shenyanming
 * @date 2020/6/14 20:54.
 */
public class OrderSerializer implements Serializer<Order> {
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
