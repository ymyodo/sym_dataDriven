package com.sym.kafka.consumer.rebalance;

import com.sym.kafka.consumer.KafkaConsumers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * 再均衡，指的是在kafka consumer所订阅的topic发生变化时发生的一种分区partition重分配机制.
 *
 *
 * @author shenyanming
 * @date 2020/6/25 10:24.
 */

public class ReBalanceConsumer {

    private final static String GROUP_NAME = "group4";

    /**
     * 根据不同序号获取同一组内的不同消费者
     */
    private KafkaConsumer<String, String> getConsumer(int number){
        Properties prop = KafkaConsumers.initProperties();
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, number);
        return KafkaConsumers.createConsumer(prop);
    }

}
