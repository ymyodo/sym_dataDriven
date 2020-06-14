package com.sym.kafka.producer.filter;

import com.sym.kafka.domain.Order;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器{@link ProducerInterceptor}
 *
 * @author shenyanming
 * @date 2020/6/14 21:09.
 */

public class OrderInterceptor implements ProducerInterceptor<String, Order> {
    @Override
    public ProducerRecord<String, Order> onSend(ProducerRecord<String, Order> record) {
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
