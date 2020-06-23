package com.sym.kafka.producer.filter;

import com.sym.kafka.domain.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Properties;

/**
 * 自定义拦截器{@link ProducerInterceptor}
 *
 * @author shenyanming
 * @date 2020/6/14 21:09.
 */

public class OrderInterceptor implements ProducerInterceptor<String, Order> {

    public static Properties properties(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.sym.kafka.producer.filter.OrderInterceptor");
        return prop;
    }

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
