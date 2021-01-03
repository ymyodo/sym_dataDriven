package com.sym.rabbitmq.delay;

import com.rabbitmq.client.*;
import com.sym.rabbitmq.RabbitmqUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 基于RabbitMQ的延迟队列配置。
 * rabbitMQ自身是没有提供延迟队列的功能, 需要通过 TTL 和 DLX 间接实现的。
 * TTL, 即time to live, 是指一个消息的过期时间;
 * DLX, 即dead letter exchanges, 消息一旦过期就会变成死信, 被转发到死信交换器上.
 *
 * @author shenyanming
 * @date 2020/5/10 9:57.
 */
@Slf4j
public class DelayQueueConfig {

    private Channel channel = RabbitmqUtil.getChannel();
    private String deadExchangeName = "dead-exchange";
    private String deadQueueName = "dead-queue";
    public static String orderExchangeName = "order-exchange";

    /**
     * 创建一个死信交换器, 它实际上就是一个普通的RabbitMQ交换器, 只不过队列中的死信消息
     * 转发它这边, 所以称它为死信交换器
     */
    public void initDeadExchange() throws IOException {
        AMQP.Exchange.DeclareOk declareOk = channel.exchangeDeclare(deadExchangeName, BuiltinExchangeType.FANOUT, false, true, null);
        log.info("创建死信交换器: {}", declareOk);
    }

    /**
     * 创建死信队列, 它用来判定死信交换器
     */
    public void initDeadQueue() throws IOException {
        // 创建死信队列
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(deadQueueName, false, false, true, null);
        log.info("创建死信队列: {}", declareOk);

        // 绑定死信交换器
        AMQP.Queue.BindOk bindOk = channel.queueBind(deadQueueName, deadExchangeName, "", null);
        log.info("绑定死信队列和死信交换器: {}", bindOk);
    }

    /**
     * 创建订单交换器, 和订单队列, 当用户下单后未支付, 就会变成死信, 被转发到死信队列上
     */
    public void createOrderQueue() throws IOException {
        // 创建订单交换器
        AMQP.Exchange.DeclareOk declareOk = channel.exchangeDeclare(orderExchangeName, BuiltinExchangeType.DIRECT, false, true, null);
        log.info("创建订单交换器：{}", declareOk);

        // 创建订单队列, 需要设置两步：
        // 1.设置队列内消息的TTL时间 (当然也可以单独设置每条消息的TTL时间, rabbitMQ会取这两个TTL的最小值, 作为最终这条消息的TTL时间)
        // 2.设置当队列出现死信消息, 需要转发的路由信息, 即
        //      x-dead-letter-exchange：队列中出现Dead Letter后将Dead Letter重新路由转发到指定 exchange（交换机）
        //      x-dead-letter-routing-key：指定routing-key发送，一般为要指定转发的队列
        Map<String, Object> argument = new HashMap<>();
        argument.put("x-dead-letter-exchange", deadExchangeName);
        argument.put("x-dead-letter-routing-key", ""); // 因为我的死信交换器类型设置为FANOUT, 所以这个routing-key设不设置都没差
        // argument.put("x-message-ttl", String.valueOf(10 * 1000)); //设置队列内消息的过期时间
        String orderQueueName = "order-queue";
        AMQP.Queue.DeclareOk info = channel.queueDeclare(orderQueueName, false, false, true, argument);
        log.info("创建订单队列：{}", info);

        // 绑定订单交换器和订单队列
        AMQP.Queue.BindOk bindOk = channel.queueBind(orderQueueName, orderExchangeName, "create", null);
        log.info("绑定订单队列和订单交换器: {}", bindOk);
    }


    /**
     * 监听死信队列, 在这个场景下, 即监听过期订单
     */
    public void receiveMessage(CountDownLatch countDownLatch) throws IOException {
        // 接收消息，3个参数的意思依次是：队列名、获取消息后是否要删除队列内的消息、接收消息的对象
        String s = channel.basicConsume(deadQueueName, true, new DefaultConsumer(channel) {
            /**
             * 这个是跟着channel一直监听的，只要有消息过来就会接收
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                log.info("消费消息死信队列(过期订单)：{}", new String(body));
                countDownLatch.countDown();
            }
        });
        log.info("监听死信队列配置：{}", s);
    }
}
