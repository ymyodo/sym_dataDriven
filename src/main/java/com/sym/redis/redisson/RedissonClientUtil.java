package com.sym.redis.redisson;

import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

import java.io.IOException;
import java.io.InputStream;

/**
 * 获取 Redisson 客户端的工具类
 * <p>
 * Created by 沈燕明 on 2019/10/14 22:09.
 */
public class RedissonClientUtil {

    @Test
    public void test() throws IOException {
        RedissonClientUtil.getSingleNodeConfig();
        RedissonClientUtil.getClusterConfig();
    }

    /**
     * 通过yaml文件获取单节点配置
     */
    public static Config getSingleNodeConfig() throws IOException {
        InputStream resourceAsStream = RedissonClientUtil.class.getClassLoader().getResourceAsStream("redisson/singleConfig.yaml");
        Config config = Config.fromYAML(resourceAsStream);
        System.out.println(config.toJSON());
        return config;
    }


    /**
     * 通过JSON文件获取集群配置
     */
    public static Config getClusterConfig() throws IOException {
        InputStream resourceAsStream = RedissonClientUtil.class.getClassLoader().getResourceAsStream("redisson/clusterConfig.json");
        Config config = Config.fromJSON(resourceAsStream);
        System.out.println(config.toJSON());
        return config;
    }


    /**
     * 通过代码配置创建一个客户端
     */
    public static RedissonClient getRedissonClient() {
        Config config = new Config();
        /*
         * config.use***()方法表示使用redis的何种方式，例如单机模式、主从模式、哨兵模式和集群模式...
         */
        config.setCodec(new JsonJacksonCodec())//序列化配置
                .setLockWatchdogTimeout(50000)//看门狗配置
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
