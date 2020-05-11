package com.sym.rabbitmq;

import com.rabbitmq.client.*;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author shenym
 * @date 2020/3/15 17:45
 */

public class RabbitmqTest {

    private Channel channel;

    /**
     * rabbit一个TCP连接开启多个信道来进行通信的，所以实际是使用channel来通信
     */
    @Before
    public void init() {
        channel = RabbitmqConnectUtil.getChannel();
    }

    /**
     * 创建交换器exchange
     */
    @Test
    @SneakyThrows
    public void createExchange() {
        // 第一个参数是交换器的名字，第二个参数是交换器的类型(BuiltinExchangeType)，第三个参数配置是否持久化，第四个参数配置是否删除，第五个参数是额外配置（用Map表示）
        AMQP.Exchange.DeclareOk declareOk = channel.exchangeDeclare("user-exchange", BuiltinExchangeType.DIRECT, true, false, null);
        System.out.println(declareOk.toString());

        // 创建交换器，但是不需要服务器返回任何信息，所以方法的返回值为void
        // channel.exchangeDeclareNoWait("user-exchange",BuiltinExchangeType.DIRECT,true,false,true,null);
    }

    /**
     * 删除交换器
     */
    @Test
    @SneakyThrows
    public void deleteExchange() {
        // 不需要任何理由，就要删除交换器，通过交换器的名称就可以删除
        AMQP.Exchange.DeleteOk deleteOk = channel.exchangeDelete("user-exchange");
        System.out.println(deleteOk.toString());

        // 如果设置为true-表示只删除未被使用的交换器；为false效果就和上面的一样
        // AMQP.Exchange.DeleteOk deleteOk1 = channel.exchangeDelete("user-exchange", true);

        // 删除交换器并且不需要服务器做任何返回,第2个参数如果为true表示只删除未被使用的交换器
        // channel.exchangeDeleteNoWait("user-exchange",true);
    }


    /**
     * 创建队列queue
     */
    @Test
    @SneakyThrows
    public void createQueue() {
        // 创建一个独占的、自动删除、不会持久化的队列，队列名称是以服务器命名的
        // AMQP.Queue.DeclareOk declareOk = channel.queueDeclare();

        // 5个参数意思依次：队列名、是否持久化、是否独占、是否自动删除、额外配置。如果队列已经存在，会返回该队列的信息
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare("user-one", true, false, false, null);
        System.out.println("创建好的队列名=" + declareOk.getQueue() + ",已存在的消息数=" + declareOk.getMessageCount() + ",消费者数=" + declareOk.getConsumerCount());

        // 创建一个不需要服务器返回任何信息的队列
        // channel.queueDeclareNoWait("user-one", true, false, false, null);

        AMQP.Queue.DeclareOk declareOk1 = channel.queueDeclarePassive("user-one");
        System.out.println("队列是否存在=" + !declareOk1.getQueue().equals(""));
    }

    /**
     * 删除队列queue
     */
    @Test
    @SneakyThrows
    public void deleteQueue() {
        // 无条件我不管就要删除这个队列，根据队列名称删除队列
        AMQP.Queue.DeleteOk deleteOk = channel.queueDelete("user-one");

        // 按条件删除队列,3个参数意思依次是：队列名、为true表示只删除未被使用的队列、为true表示只删除没有任何消息的空队列
        // AMQP.Queue.DeleteOk deleteOk1 = channel.queueDelete("user-one", true, true);

        // 按条件删除队列,参数意思跟上面的意义，只不过它不需要服务器返回任何信息
        //channel.queueDeleteNoWait("user-one", true, true);
    }

    /**
     * 绑定交换器和交换器
     */
    @Test
    @SneakyThrows
    public void bindBothExchange() {
        // 先模拟创建2个交换器
        channel.exchangeDeclare("test-one", BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare("test-two", BuiltinExchangeType.DIRECT);

        // 绑定交换器和交换器，4个参数依次是：目的交换器、源交换器、路由键、额外参数
        AMQP.Exchange.BindOk bindOk = channel.exchangeBind("test-two", "test-one", "one", null);
        System.out.println(bindOk.toString());

        // 绑定交换器和交换器，且不需要服务器做任何返回
        // channel.exchangeBindNoWait("test-two", "test-one", "one",null);
    }

    /**
     * 解绑交换器和交换器
     */
    @Test
    @SneakyThrows
    public void unbindBothExchange() {
        // 将交换器解绑，4个参数依次是：目的交换器、源交换器、路由键、额外参数
        AMQP.Exchange.UnbindOk unbindOk = channel.exchangeUnbind("test-two", "test-one", "one", null);
        System.out.println(unbindOk.toString());

        // 解绑交换器和交换器，且不需要服务器做任何返回
        // channel.exchangeUnbindNoWait("test-two", "test-one", "one", null);
    }

    /**
     * 绑定交换器和队列
     */
    @Test
    @SneakyThrows
    public void bindExchangeAndQueue() {
        // 绑定交换器和队列，4个参数意思依次是：队列名、交换器名、路由键、额外参数
        AMQP.Queue.BindOk bindOk = channel.queueBind("user-one", "user-exchange", "one", null);
        System.out.println(bindOk.toString());

        // 绑定交换器和队列，且不需要也不用等待服务器的返回信息
        // channel.queueBindNoWait("user-one", "user-exchange", "one", null);
    }

    /**
     * 解绑交换器和队列
     */
    @Test
    @SneakyThrows
    public void unbindExchangeAndQueue() {
        // 解绑交换器和队列，4个参数的意思依次是：队列名、交换器名、路由键、额外参数
        AMQP.Queue.UnbindOk unbindOk = channel.queueUnbind("user-one", "user-exchange", "one", null);
        System.out.println(unbindOk.toString());

    }

    /**
     * 发送消息
     */
    @Test
    @SneakyThrows
    public void sendMessage() {
        // 待发送的消息
        String msg = "{\"name\":\"张三\",\"id\":\"3146017029\",\"isDel\":false}";
        // 配置待发送消息的额外属性如消息头等等
//            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
//                    .contentType("text.json")
//                    .contentEncoding("utf-8")
//                    .timestamp(new Date()).build();
        // 发送消息，6个为最全参数，意思依次是：交换器名、路由键、是否强制发送、是否立即发送、消息的其他属性如消息头等、消息内容（转成字节数组）
        // channel.basicPublish("", "", false, false, props, msg.getBytes());

        // 发送消息
        channel.basicPublish("user-exchange", "one", null, msg.getBytes());
        System.out.println("消息发送成功~~");
    }

    /**
     * 接收消息
     */
    @Test
    @SneakyThrows
    public void receiveMessage() {
        // 接收消息，3个参数的意思依次是：队列名、获取消息后是否要删除队列内的消息、接收消息的对象
        String s = channel.basicConsume("user-one", true, new DefaultConsumer(channel) {
            /**
             * 这个是跟着channel一直监听的，只要有消息过来就会接收
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("envelope=" + envelope.toString());
                System.out.println("properties=" + properties.toString());
                System.out.println("收到的消息为=" + new String(body));
            }
        });
        System.out.println("服务器返回的信息：" + s);
    }

    /**
     * 当消息成功被交换器Exchange接收到，就会返回true，其它返回false。
     * 所以需要保证交换器有绑定队列，不然数据虽然到达交换器，但是没有到达队列，理论上还是丢失的
     */
    @Test
    @SneakyThrows
    public void askConfirmTest() {
        // 配置此通道先开启消息发送确认机制
        AMQP.Confirm.SelectOk selectOk = channel.confirmSelect();
        // 向rabbitMQ的交换器发送消息
        String msg = "{\"name\":\"张三\",\"id\":\"3146017029\",\"isDel\":false}";
        channel.basicPublish("user-exchange", "one", null, msg.getBytes());
        System.out.println(channel.waitForConfirms());
    }

    /**
     * rabbitMQ的连接connection和信道channel都是长连接，通常情况下不要关闭，保持连接状态，除非你再也不用了
     * 在测试中为了可以连续执行不同的API，默认我都关闭了，而且关闭的是连接connection，只有把连接都关闭了，才算真正关闭
     */
    @After
    public void closeAll() {
        RabbitmqConnectUtil.closeAll();
    }
}
