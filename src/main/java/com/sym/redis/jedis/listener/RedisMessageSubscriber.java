package com.sym.redis.jedis.listener;

import com.sym.redis.jedis.singleNode.JedisPoolUtil;
import redis.clients.jedis.Jedis;
/**
 * 用java实现Redis 超时失效 key 的监听触发
 * 
 * 订阅者
 * @author 沈燕明
 *
 */
public class RedisMessageSubscriber {
	
	public static void main(String[] args) {  
        Jedis jedis = JedisPoolUtil.getJedis();
        while(true){
        	jedis.subscribe(new RedisMessageListener(), "__keyevent@0__:expired");  
        }
    }  

}
