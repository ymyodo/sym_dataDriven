package com.sym.redis.jedis.listener;
import com.sym.redis.jedis.singleNode.JedisPoolUtil;
import redis.clients.jedis.Jedis;
/**
 * 测试类
 * @author 沈燕明
 *
 */
public class RedisTest {

	public static void main(String[] args) {
		Jedis jedis = JedisPoolUtil.getJedis();
		jedis.set("key1", "新浪微博：小叶子一点也不逗");  
        jedis.expire("notify", 10);  
//        jedis.set("key2", "新浪微博：小叶子一点也不逗");  
//        jedis.expire("notify", 10);  

	}

}
