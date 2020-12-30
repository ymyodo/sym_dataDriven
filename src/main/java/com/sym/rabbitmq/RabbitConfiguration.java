package com.sym.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static com.rabbitmq.client.BuiltinExchangeType.DIRECT;

/**
 * rabbitMQ配置参数类
 *
 * @author shenyanming
 * @date 2020/12/13 12:18.
 */
@Data
@Builder
@NoArgsConstructor
public class RabbitConfiguration {

    @Data
    @Builder
    public static class ExchangeConfig {
        /**
         * 交换器名称
         */
        private String name = "default";

        /**
         * 交换器类型
         */
        private BuiltinExchangeType type = DIRECT;

        /**
         * 交换器是否持久化
         */
        private boolean durable = true;

        /**
         * 交换器是否自动删除, 如果该交换器没有队列或其他交换器与其绑定,
         * rabbitMQ自动删除该交换器
         */
        private boolean autoDelete = false;

        /**
         * 如果设置为true表示是内置交换器, 客户端程序无法直接发送
         * 消息到这个交换器, 只能通过交换器路由到交换器中
         */
        private boolean internal = false;

        /**
         * 交换器额外参数
         */
        private Map<String, Object> arguments;
    }

    @Data
    @Builder
    public static class QueueConfig {

        /**
         * 队列名
         */
        private String name = "default";

        /**
         * 是否持久化
         */
        private boolean durable = true;

        /**
         * 为true表示此队列排他, 如果一个队列被声明为排他队列, 则该队列仅对首次声明它的连接可见, 并在连接断开时自动删除.
         * 1.排他队列是基于{@link Connection} 可见, 同一个连接的不同{@link Channel}可以同时访问同一连接创建的排他队列;
         * 2."首次"是指如果一个{@link Connection}已经声明了一个排他队列, 其它{@link Connection}是不允许创建同名的排他队列;
         * 3.即使排他队列设置为持久化的, 一旦连接关闭和客户端退出, 该排他队列都会被自动删除, 所以这种队列适用于一个客户端同时发送和读取消息的应用场景
         */
        private boolean exclusive = false;

        /**
         * 当这个队列的所有消费者都
         */
        private boolean autoDelete = false;

        /**
         * 额外参数
         */
        private Map<String, Object> arguments;
    }

    @Data
    @Builder
    public static class ExchangeBindingConfig {
        /**
         * 源交换器
         */
        private String source;

        /**
         * 目标交换器
         */
        private String target;

        /**
         * 路由键
         */
        private String routingKey;

        /**
         * 额外参数
         */
        private Map<String, Object> arguments;

    }

    @Data
    @Builder
    public static class QueueBindingConfig{

        /**
         * 交换器名称
         */
        private String exchangeName;

        /**
         * 队列名称
         */
        private String queueName;

        /**
         * 路由键
         */
        private String routingKey;

        /**
         * 额外参数
         */
        private Map<String, Object> arguments;
    }

    @Data
    @Builder
    public static class ProductConfig {

        /**
         * Producer发送消息给交换器, 而交换器无法找到符合投递条件的队列时, 若此参数：
         * - true, RabbitMQ会调用Basic.Return命令将消息返回给生产者, 通过{@link Channel#addReturnListener(ReturnListener)}可以监听返回的消息;
         * - false, RabbitMQ会将消息丢弃;
         */
        private boolean mandatory = true;

        /**
         * 此参数设置为true, 当交换器投递消息到队列时, 发现队列上不存在任何消费者, 那么该消息
         * 就不会存入队列中, 该条消息会桶Basic.Return返回给生产者.
         *
         * RabbitMQ 3.0版本去掉了对此参数的支持, 转而建议采用TTL和DLX方法替代.
         */
        private boolean immediate = false;
    }
}
