package com.sym.kafka.producer.transaction;

import com.sym.kafka.producer.KafkaProducers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author shenyanming
 * @date 2020/7/2 21:51.
 */

public class ProducerTransaction {


    public static void main(String[] args) {
        Properties prop = KafkaProducers.initProperties();
        prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "123456789");

        // 创建生产者
        KafkaProducer<String, String> producer = KafkaProducers.createProducer(prop);
        // 初始化事务
        producer.initTransactions();
        // 开启事务
        producer.beginTransaction();
        try {
            // doSomething
            producer.send(null);
            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            // 回滚事务
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
