package com.sym.kafka.producer;

import com.sym.kafka.constant.KafkaConstant;
import com.sym.util.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author shenyanming
 * @date 2020/6/10 20:40.
 */
@Slf4j
public class KafkaProducerTest {

    private KafkaProducer<String, String> producer;

    @Before
    public void init(){
        producer = KafkaProducers.createProducer();
    }

    /**
     * 同步发送数据
     */
    @Test
    public void test01() throws ExecutionException, InterruptedException {
        // 创建消息体
        ProducerRecord<String, String> record = KafkaProducers.createRecord(KafkaConstant.TOPIC_NAME, "yeah!kafka");

        // 发送消息, 消息发送成功, kafka broker会响应分片信息、偏移量..., 发送失败则直接报错
        Future<RecordMetadata> future = producer.send(record);

        // 获取响应
        RecordMetadata metadata = future.get();
        log.info("响应：{}", JSONUtil.toJson(metadata));
    }

    /**
     * 异步发送数据
     */
    @Test
    public void test02(){
        // 创建消息体
        ProducerRecord<String, String> record = KafkaProducers.createRecord(KafkaConstant.TOPIC_NAME, "yeah!kafka");

        // 异步发送数据
        producer.send(record, (metadata, exception)->{
            log.info("kafka异步发送消息响应, metadata: {}, exception: {}", metadata, exception);
            if(Objects.isNull(exception)){
                // 只有当异常为空时, 才表示消息发送成功
                log.info("响应信息：{}", JSONUtil.toJson(metadata));
            }
        });


    }


}
