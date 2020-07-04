package com.sym.kafka.consumer.interceptor;

import com.sym.kafka.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author shenyanming
 * @date 2020/6/25 10:48.
 */

public class OrderConsumerInterceptor implements ConsumerInterceptor<String, Order> {
    @Override
    public ConsumerRecords<String, Order> onConsume(ConsumerRecords<String, Order> records) {
        return null;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
