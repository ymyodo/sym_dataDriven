package com.sym.kafka.consumer;

import com.sym.kafka.constant.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author shenyanming
 * @date 2020/6/23 21:36.
 */
@Slf4j
public class KafkaConsumerTest {

    /**
     * 设置kafka consumer自动提交偏移量
     */
    @Test
    public void autoCommit() throws InterruptedException {
        Properties props = KafkaConsumers.initProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaConstant.AUTO_COMMIT_TRUE);
        // 创建消费者
        KafkaConsumer<String, String> consumer = KafkaConsumers.createConsumer(props);
        // 订阅默认主题
        consumer.subscribe(Pattern.compile(KafkaConstant.TOPIC_NAME));

        for (; ; ) {
            Thread.sleep(500);
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> {
                log.info("偏移量：{}, 消息内容：{}", record.offset(), record.value());
            });
        }
    }

    /**
     * 设置kafka consumer同步提交偏移量
     */
    @Test
    public void syncCommit() throws InterruptedException {
        // 设置不同的消费组
        Properties props = KafkaConsumers.initProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        // 创建消费者
        KafkaConsumer<String, String> consumer = KafkaConsumers.createConsumer(props);
        // 订阅默认主题
        consumer.subscribe(Pattern.compile(KafkaConstant.TOPIC_NAME));
        for (; ; ) {
            Thread.sleep(500);
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> {
                log.info("偏移量：{}, 消息内容：{}", record.offset(), record.value());
                consumer.commitSync();
            });
        }
    }

    /**
     * 设置kafka consumer异步提交偏移量
     */
    @Test
    public void asyncCommit() throws InterruptedException {
        // 设置不同的消费组
        Properties props = KafkaConsumers.initProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");

        // 创建消费者
        KafkaConsumer<String, String> consumer = KafkaConsumers.createConsumer(props);
        // 订阅默认主题
        consumer.subscribe(Pattern.compile(KafkaConstant.TOPIC_NAME));
        for (; ; ) {
            Thread.sleep(500);
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> {
                log.info("偏移量：{}, 消息内容：{}", record.offset(), record.value());
                consumer.commitAsync((offsets, exception) -> {
                    if (Objects.isNull(exception)) {
                        log.info("异步提交结果:{}", offsets);
                    }
                });
            });
        }
    }

    /**
     * 手动指定从哪个偏移量开始读取
     */
    @Test
    public void seek() throws InterruptedException {
        // 设置不同的消费组
        Properties props = KafkaConsumers.initProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group3");

        // 创建消费者
        KafkaConsumer<String, String> consumer = KafkaConsumers.createConsumer(props);
        // 订阅默认主题
        consumer.subscribe(Pattern.compile(KafkaConstant.TOPIC_NAME));

        // 消费三次
        for (int i = 0; i < 3; i++) {
            Thread.sleep(500);
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            consumerRecords.forEach(record -> {
                log.info("偏移量：{}, 消息内容：{}", record.offset(), record.value());
                consumer.commitSync();
            });
        }

        // 然后重新
        consumer.seek(new TopicPartition(KafkaConstant.TOPIC_NAME, 0), 0);
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
        consumerRecords.forEach(record -> {
            log.info("偏移量：{}, 消息内容：{}", record.offset(), record.value());
            consumer.commitSync();
        });
    }
}
