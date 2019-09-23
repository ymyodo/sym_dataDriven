package com.sym.redis.redisson;

import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Redis的Java客户端-Redisson。
 * 首先要明确一点：Redisson并不是跟Jedis一样，仅仅是为了操作redis而做的一些命令封装，它在这种基础上，完善redis在分布式系统的运用
 * 例如分布式锁！所以最好在使用更高场景时再来使用Redisson，官方文档地址：https://github.com/redisson/redisson/wiki
 *
 * Created by shenym on 2019/9/23.
 */
public class RedissonDemo {

    /**
     * 设置key、获取key、修改Key、删除Key
     */
    @Test
    public void testOne(){
        RedissonClient client = this.getInstance();

        /*
         * 设置一个Key
         */
        RBucket<String> bucket = client.getBucket("redisson:my-test");
        bucket.set("sunshine girl");
        bucket.expire(1, TimeUnit.MINUTES);//设置过期时间

        /*
         * 获取key的值
         */
        String s = bucket.get();
        System.out.println(s);

        /*
         * 修改key的值直接再设置即可
         * 它还支持CAS更改
         */
        boolean b = bucket.compareAndSet("old", "new value by cas");
        System.out.println(b?"修改成功~":"修改失败");

        /*
         * 删除key
         */
        bucket.delete();
    }

    /**
     * 获取一个Redisson客户操作端
     */
    private RedissonClient getInstance() {
        Config config = new Config();
        /*
         * config.use***()方法表示使用redis的何种方式，例如单机模式、主从模式、哨兵模式和集群模式...
         */
        config.setCodec(new JsonJacksonCodec())
                .useSingleServer().setAddress("redis://127.0.0.1:6379")
                .setDatabase(0)
                .setConnectionPoolSize(10)
                .setConnectionMinimumIdleSize(10)
                //.setClientName("客户端测试一号")
                .setConnectTimeout(30);
        RedissonClient redissonClient = Redisson.create(config);
        try {
            String s = redissonClient.getConfig().toJSON();
            System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return redissonClient;
    }

}
