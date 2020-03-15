package com.sym.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Auther: shenym
 * @Date: 2018-12-10 14:29
 */
public class RabbitMQConnectUtil {

    private static Connection connection;
    private static ThreadLocal<Channel> threadLocal;

    static {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setEncoding("utf-8");
        try {
            // 加载配置文件
            configuration.load("rabbitmq.properties");
            String username = configuration.getString("username");
            String password = configuration.getString("password");
            String host = configuration.getString("host");
            int port = configuration.getInt("port");
            // 初始化连接
            ConnectionFactory factory = new ConnectionFactory();
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
            e.printStackTrace();
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
     * 获取一个通道
     */
    public static Channel getChannel() {
        Channel channel = threadLocal.get();
        if (channel == null) {
            try {
                channel = connection.createChannel();
                threadLocal.set(channel);
            } catch (IOException e) {
                e.printStackTrace();
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
    public static void close() {
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
    
    
    // 测试连接
    public static void main(String[] args){
        new Thread(()->{
            Channel channel = RabbitMQConnectUtil.getChannel();
            System.out.println(Thread.currentThread().getName()+"获取的通道："+channel.toString()+",是否开启？"+channel.isOpen());
            RabbitMQConnectUtil.close();
            System.out.println(Thread.currentThread().getName()+"获取的通道："+channel.toString()+",是否开启？"+channel.isOpen());
        },"线程1").start();

        new Thread(()->{
            Channel channel = RabbitMQConnectUtil.getChannel();
            System.out.println(Thread.currentThread().getName()+"获取的通道："+channel.toString()+",是否开启？"+channel.isOpen());
            //RabbitMQConnectUtil.close();
            System.out.println(Thread.currentThread().getName()+"获取的通道："+channel.toString()+",是否开启？"+channel.isOpen());
        },"线程2").start();
    }
}
