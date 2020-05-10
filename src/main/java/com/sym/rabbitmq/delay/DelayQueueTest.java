package com.sym.rabbitmq.delay;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import com.sym.rabbitmq.RabbitMQConnectUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author shenyanming
 * @date 2020/5/10 10:35.
 */

public class DelayQueueTest {

    private Channel channel;
    private DelayQueueConfig config;

    @Test
    public void send() throws IOException, InterruptedException {
        // 设置监听器
        CountDownLatch countDownLatch = new CountDownLatch(1);
        config.receiveMessage(countDownLatch);

        // 设置此消息的过期时间, 单位毫秒
        String expiration = String.valueOf(20 * 1000);
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().expiration(expiration).build();
        // 发送消息
        channel.basicPublish(DelayQueueConfig.orderExchangeName, "create", basicProperties, "orderId:123".getBytes());

        countDownLatch.await();
    }



    @Before
    public void init() throws IOException {
        channel = RabbitMQConnectUtil.getChannel();
        // 初始化必要的交换器和队列
        config = new DelayQueueConfig();
        config.initDeadExchange();
        config.initDeadQueue();
        config.createOrderQueue();
    }

    /**
     * rabbitMQ的连接connection和信道channel都是长连接，通常情况下不要关闭，保持连接状态，除非你再也不用了
     * 在测试中为了可以连续执行不同的API，默认我都关闭了，而且关闭的是连接connection，只有把连接都关闭了，才算真正关闭
     */
    @After
    public void closeAll() {
        RabbitMQConnectUtil.closeAll();
    }
}
