package com.sym.rabbitmq;

import com.google.gson.Gson;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * RabbitMQ工具类
 *
 * @author shenyanming
 * @date 2020/12/29 21:10.
 */
@Slf4j
public class RabbitmqUtil {

    /**
     * Connection可以创建多个Channel实例, 但是channel实例不能在多个线程间共享, 所以要为每个线程开辟一个独立的Channel
     */
    private static Connection connection;
    private static ThreadLocal<Channel> threadLocal;

    private static Gson gson = new Gson();

    static {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setEncoding("utf-8");
        try {
            // 加载配置文件
            configuration.load("properties/rabbitmq.properties");
            String username = configuration.getString("username");
            String password = configuration.getString("password");
            String host = configuration.getString("host");
            int port = configuration.getInt("port");
            // 初始化连接
            ConnectionFactory factory = new ConnectionFactory();
            // 或者直接设置uri factory.setUri("amqp://username:password@ipAddress:port/virtualHost");
            // 用户及其密码为guest,只适用于本地访问
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setHost(host);
            factory.setPort(port);
            // 设置rabbitMQ的虚拟主机,默认为/
            factory.setVirtualHost("/");
            connection = factory.newConnection();
            //线程本地变量
            threadLocal = new ThreadLocal<>();
        } catch (ConfigurationException | IOException | TimeoutException e) {
            log.error("初始化失败, ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取一个通道
     */
    public static Channel getChannel() {
        Channel channel = threadLocal.get();
        if (channel == null) {
            try {
                channel = connection.createChannel();
                threadLocal.set(channel);
            } catch (IOException e) {
                // 暂时以运行时异常抛出
                throw new RuntimeException("创建rabbitMQ连接通道失败");
            }
        }
        // 防止用户直接使用channel关闭，导致在threadLocal中的channel其实是关闭的
        if (!channel.isOpen()) {
            threadLocal.remove();
            try {
                channel = connection.createChannel();
                threadLocal.set(channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return channel;
    }


    /**
     * 关闭一个通道
     */
    public static void closeChannel() {
        Channel channel = threadLocal.get();
        if (channel != null) {
            try {
                // 防止用户直接使用channel关闭
                if (channel.isOpen()) {
                    channel.close();
                    threadLocal.remove();
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭所有的channel通道，注意此方法会关闭所有的连接
     */
    public static void closeAll() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建交换器
     *
     * @param config 交换器配置
     */
    public static void createExchange(RabbitConfiguration.ExchangeConfig config) {
        Objects.requireNonNull(config, "config is null");
        Channel channel = getChannel();
        try {
            channel.exchangeDeclare(config.getName(), config.getType(), config.isDurable(), config.isAutoDelete(), config.getArguments());
            // 创建交换器，但是不需要服务器返回任何信息，所以方法的返回值为void
            // channel.exchangeDeclareNoWait("user-exchange",BuiltinExchangeType.DIRECT,true,false,true,null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除交换器
     *
     * @param exchangeName 交换器名称
     */
    public void deleteExchange(String exchangeName) {
        try {
            // 不需要任何理由，就要删除交换器，通过交换器的名称就可以删除
            AMQP.Exchange.DeleteOk deleteOk = getChannel().exchangeDelete(exchangeName);
            System.out.println(deleteOk.toString());

            // 如果设置为true-表示只删除未被使用的交换器；为false效果就和上面的一样
            // AMQP.Exchange.DeleteOk deleteOk1 = channel.exchangeDelete("user-exchange", true);

            // 删除交换器并且不需要服务器做任何返回,第2个参数如果为true表示只删除未被使用的交换器
            // channel.exchangeDeleteNoWait("user-exchange",true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建队列
     *
     * @param config 队列配置
     */
    public static void createQueue(RabbitConfiguration.QueueConfig config) {
        Objects.requireNonNull(config, "config is null");
        Channel channel = getChannel();
        try {
            channel.queueDeclare(config.getName(), config.isDurable(), config.isExclusive(), config.isAutoDelete(), config.getArguments());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除队列
     *
     * @param queueName 队列名称
     */
    public static void deleteQueue(String queueName) {
        try {
            // 无条件我不管就要删除这个队列，根据队列名称删除队列
            AMQP.Queue.DeleteOk deleteOk = getChannel().queueDelete(queueName);

            // 按条件删除队列,3个参数意思依次是：队列名、为true表示只删除未被使用的队列、为true表示只删除没有任何消息的空队列
            // AMQP.Queue.DeleteOk deleteOk1 = channel.queueDelete("user-one", true, true);

            // 按条件删除队列,参数意思跟上面的意义，只不过它不需要服务器返回任何信息
            //channel.queueDeleteNoWait("user-one", true, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 绑定交换器
     */
    public static void exchangeBinding(RabbitConfiguration.ExchangeBindingConfig config) {
        Objects.requireNonNull(config, "config is null");
        try {
            // 绑定交换器和交换器，4个参数依次是：目的交换器、源交换器、路由键、额外参数
            AMQP.Exchange.BindOk bindOk = getChannel().exchangeBind(config.getTarget(), config.getSource(), config.getRoutingKey(), config.getArguments());

            // 绑定交换器和交换器，且不需要服务器做任何返回
            // channel.exchangeBindNoWait("test-two", "test-one", "one",null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 解绑交换器
     */
    public static void exchangeUnBinding(RabbitConfiguration.ExchangeBindingConfig config) {
        Objects.requireNonNull(config, "config is null");
        try {
            // 将交换器解绑，4个参数依次是：目的交换器、源交换器、路由键、额外参数
            AMQP.Exchange.UnbindOk unbindOk = getChannel().exchangeUnbind(config.getTarget(), config.getSource(), config.getRoutingKey(), config.getArguments());

            // 解绑交换器和交换器，且不需要服务器做任何返回
            // channel.exchangeUnbindNoWait("test-two", "test-one", "one", null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 绑定队列和交换器
     */
    public static void queueBinding(RabbitConfiguration.QueueBindingConfig config) {
        try {
            // 绑定交换器和队列，4个参数意思依次是：队列名、交换器名、路由键、额外参数
            AMQP.Queue.BindOk bindOk = getChannel().queueBind(config.getQueueName(), config.getExchangeName(), config.getRoutingKey(), config.getArguments());

            // 绑定交换器和队列，且不需要也不用等待服务器的返回信息
            // channel.queueBindNoWait("user-one", "user-exchange", "one", null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 解绑队列和交换器
     *
     * @param config 配置
     */
    public static void queueUnBinding(RabbitConfiguration.QueueBindingConfig config) {
        try {
            // 解绑交换器和队列，4个参数的意思依次是：队列名、交换器名、路由键、额外参数
            AMQP.Queue.UnbindOk unbindOk = getChannel().queueUnbind(config.getQueueName(), config.getExchangeName(), config.getRoutingKey(), config.getArguments());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 发送消息
     *
     * @param exchange 交换器
     * @param route    路由键
     * @param msg      消息体
     */
    public static void send(String exchange, String route, Object msg) {
        try {
            // 配置待发送消息的额外属性如消息头等等
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().contentType("text.json").contentEncoding("utf-8").timestamp(new Date()).build();
            // 发送消息，6个为最全参数，意思依次是：交换器名、路由键、是否强制发送、是否立即发送、消息的其他属性如消息头等、消息内容（转成字节数组）
            // channel.basicPublish("", "", false, false, props, msg.getBytes());

            // 发送消息
            getChannel().basicPublish(exchange, route, props, toBytes(msg));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 发送消息, 同时等待broker返回.
     * 当消息成功被交换器Exchange接收到, 就会返回true, 其它返回false.
     * 所以需要保证交换器有绑定队列, 不然数据虽然到达交换器, 但是没有到达队列, 理论上还是丢失.
     *
     * @param exchange 交换器
     * @param route    路由键
     * @param msg      消息体
     */
    public static boolean sendAck(String exchange, String route, Object msg) {
        Channel channel = getChannel();
        try {
            // 配置此通道先开启消息发送确认机制
            AMQP.Confirm.SelectOk selectOk = channel.confirmSelect();
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().contentType("text.json").contentEncoding("utf-8").timestamp(new Date()).build();
            channel.basicPublish(exchange, route, props, toBytes(msg));
            // 串行等待rabbitmq响应, 实际效果其实和事务差不多
            return channel.waitForConfirms();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 接收消息
     *
     * @param queue 队列名称
     * @param type  消息体类型
     */
    public static <T> void receive(String queue, Class<T> type, Consumer<T> consumer) {
        Channel channel = getChannel();
        try {
            // 接收消息，3个参数的意思依次是：队列名、获取消息后是否要删除队列内的消息、接收消息的对象
            channel.basicConsume(queue, true, new DefaultConsumer(channel) {
                /**
                 * 这个是跟着channel一直监听的, 只要有消息过来就会接收
                 */
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    consumer.accept(gson.fromJson(new String(body, StandardCharsets.UTF_8), type));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] toBytes(Object msg) {
        return gson.toJson(msg).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 内部封装的方法调用类, 减少try-catch处理逻辑
     */
    private static class Handler {

        private Throwable t;

        private static Handler instance() {
            return new Handler();
        }

        public static Handler run(Runnable runnable) {
            Handler handler = Handler.instance();
            try {
                runnable.run();
            } catch (Throwable t) {
                handler.t = t;
            }
            return handler;
        }

        private Handler() {
        }

        public void exceptionally(Consumer<Throwable> consumer) {
            if (Objects.nonNull(t)) {
                consumer.accept(t);
            }
        }
    }
}
