package com.sym.redis.jedis.singleNode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * jedis连接池配置
 * 
 * @author 沈燕明
 *
 */
public class JedisPoolUtil {

	private final static JedisPool POOL;
	private static String HOST = "127.0.0.1";
	private static int PORT = 6379;
	static{
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(10);
		POOL = new JedisPool(config, HOST, PORT);
	}
	public static Jedis getJedis(){
		return POOL.getResource();
	}

}
