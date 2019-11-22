package com.sym.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Java对RabbitMQ的进阶操作
 *
 * Created by 沈燕明 on 2019/6/4 13:57.
 */
public class RabbitMQAdvancedDemo {

    private Channel channel;

    @Before
    public void init(){
        this.channel = RabbitMQConnectUtil.getChannel();
    }

    /**
     * 当消息成功被交换器Exchange接收到，就会返回true，其它返回false。
     * 所以需要保证交换器有绑定队列，不然数据虽然到达交换器，但是没有到达队列，理论上还是丢失的
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void askConfirmTest() throws IOException, InterruptedException {
        // 配置此通道先开启消息发送确认机制
        AMQP.Confirm.SelectOk selectOk = channel.confirmSelect();
        // 向rabbitMQ的交换器发送消息
        String msg = "{\"name\":\"张三\",\"id\":\"3146017029\",\"isDel\":false}";
        channel.basicPublish("user-exchange", "one", null, msg.getBytes());
        System.out.println(channel.waitForConfirms());
    }
}
