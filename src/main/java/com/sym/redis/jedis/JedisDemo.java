package com.sym.redis.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Auther: shenym
 * @Date: 2019-03-28 11:46
 */
public class JedisDemo {

    /**
     * 单节点访问
     */
    @Test
    public void testOne(){
        // 连接单个redis节点
        Jedis jedis = new Jedis("127.0.0.1",6379);
        jedis.auth("root");//redis访问密码
        System.out.println(jedis.ping());
    }

    /**
     * 单节点访问
     * 使用连接池
     */
    @Test
    public void testTwo(){
        // 连接配置
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMinIdle(5);
        // 创建连接池
        JedisPool jedisPool = new JedisPool(poolConfig,"127.0.0.1",6379);
        Jedis jedis = jedisPool.getResource();
        System.out.println(jedis.ping());
        jedis.close();
    }


    /**
     * 集群访问
     */
    @Test
    public void testThree(){
        // 只需要指定集群内的一个节点即可
        HostAndPort hostAndPort = new HostAndPort("10.23.119.56",6379);
        JedisCluster jedisCluster = new JedisCluster(hostAndPort);
        Map<String, JedisPool> nodes = jedisCluster.getClusterNodes();
        nodes.forEach((a,b)->{
            System.out.println("节点信息=>"+a);
        });
    }


    /**
     * 集群访问
     * 使用连接池
     */
    @Test
    public void testFour(){
        // 连接配置
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMinIdle(5);
        HostAndPort hostAndPort = new HostAndPort("10.23.119.56",6379);
        // 创建连接池
        JedisCluster jedisCluster = new JedisCluster(hostAndPort,poolConfig);
        Map<String, JedisPool> nodes = jedisCluster.getClusterNodes();
        nodes.forEach((a,b)->{
            System.out.println("节点信息=>"+a);
        });
    }

    /**
     * jedis发布与订阅
     */
    @Test
    public void testFive(){
        Jedis jedis = new Jedis("127.0.0.1",6379);
        jedis.auth("root");
        JedisPubSub jedisPubSub = new JedisPubSub() {
            @Override
            public void subscribe(String... channels) {
                super.subscribe(channels);
            }

            @Override
            public void onMessage(String channel, String message) {
                System.out.println(channel);
                System.out.println(message);
            }
        };
        jedis.subscribe(jedisPubSub,"redisLock");

    }
    
    public static void main(String[] args){

    }
}
