package com.sym.redis;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Test;


import java.util.concurrent.ExecutionException;

/**
 * @Auther: shenym
 * @Date: 2019-03-29 15:18
 */
public class LettuceTest {

    /**
     * 单机模式-redisClient
     * 同步操作
     */
    @Test
    public void syncWithNode() {
        RedisURI redisURI = RedisURI.builder().withHost("127.0.0.1").withPort(6379).withPassword("root").build();
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connect = redisClient.connect();
        RedisCommands<String, String> commands = connect.sync();
        String s = commands.get("label:1");
        System.out.println(s);
    }


    /**
     * 单机模式-redisClient
     * 异步操作
     */
    @Test
    public void asyncWithNode() {
        RedisURI redisURI = RedisURI.builder().withHost("127.0.0.1").withPort(6379).withPassword("root").build();
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connect = redisClient.connect();
        RedisAsyncCommands<String, String> commands = connect.async();
        RedisFuture<String> redisFuture = commands.get("label:1");
        try {
            String s = redisFuture.get();
            System.out.println(s);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }


    /**
     * 集群模式-RedisClusterClient
     */
    @Test
    public void cluster() {
        RedisURI redisURI = RedisURI.builder().withHost("10.23.119.56").withPort(6379).build();
        RedisClusterClient clusterClient = RedisClusterClient.create(redisURI);
        StatefulRedisClusterConnection<String, String> connect = clusterClient.connect();
        RedisAdvancedClusterCommands<String, String> commands = connect.sync();
        String name = commands.get("name");
        System.out.println(name);
    }
}
