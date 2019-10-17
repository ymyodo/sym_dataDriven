package com.sym.redis.redisson;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

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
        RedissonClient client = RedissonClientUtil.getRedissonClient();

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


}
