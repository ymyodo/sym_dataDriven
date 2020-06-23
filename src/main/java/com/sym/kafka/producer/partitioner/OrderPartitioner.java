package com.sym.kafka.producer.partitioner;

import com.sym.kafka.constant.KafkaConstant;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Properties;

/**
 * 自定义的分区器{@link Partitioner}
 *
 * @author shenyanming
 * @date 2020/6/14 20:55.
 */

public class OrderPartitioner implements Partitioner {

    public static Properties properties(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.sym.kafka.producer.partitioner.OrderPartitioner");
        return prop;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
