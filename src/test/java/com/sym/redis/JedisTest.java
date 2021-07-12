package com.sym.redis;

import com.sym.redis.jedis.singleNode.JedisPoolUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * jedis测试类
 *
 * @author shenym
 * @date 2019-03-28 11:46
 */
public class JedisTest {

    private Jedis jedis;

    @Before
    public void Init() {
        jedis = JedisPoolUtil.getJedis();
    }

    @Test
    public void set() {
        String str = "沈燕明,2018-02-05";
        jedis.set("str", str);
    }

    @Test
    public void get() {
        String str = jedis.get("str");
        System.out.println(str);
    }

    @Test
    public void del() {
        Long del = jedis.del("str");
        System.out.println("删除了..." + del + "条");
    }

    @Test
    public void getAndSet() {
        String str = jedis.getSet("str", "沈燕明你是最棒的");
        System.out.println(str);
        String strNew = jedis.get("str");
        System.out.println(strNew);

    }

    @Test
    public void incr() {
        Long incr = jedis.incr("count");
        System.out.println("返回加完后的结果：" + incr);
    }

    /**
     * 单节点访问
     */
    @Test
    public void initialize(){
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
    public void jedisPool(){
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
    public void cluster(){
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
    public void clusterFromPool(){
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
    public void pulAndSub(){
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
}
