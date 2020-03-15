package com.sym.redis.jedis.listener;
import redis.clients.jedis.JedisPubSub;
/**
 * 用java实现Redis 超时失效key 的监听触发
 *
 * 监听器
 * 
 * @author 沈燕明
 *
 */
public class RedisMessageListener extends JedisPubSub {
	
	@Override  
    public void unsubscribe() {  
        super.unsubscribe();  
    }  
  
    @Override  
    public void unsubscribe(String... channels) {  
        super.unsubscribe(channels);  
    }  
  
    @Override  
    public void subscribe(String... channels) {  
        super.subscribe(channels);  
    }  
  
    @Override  
    public void psubscribe(String... patterns) {  
        super.psubscribe(patterns);  
    }  
  
    @Override  
    public void punsubscribe() {  
        super.punsubscribe();  
    }  
  
    @Override  
    public void punsubscribe(String... patterns) {  
        super.punsubscribe(patterns);  
    }  
  
    @Override  
    public void onMessage(String channel, String message) {  
    	
    	
        System.out.println("这个键要过期了 :" + message);  
        // this.unsubscribe();  
    }  
  
    @Override  
    public void onPMessage(String pattern, String channel, String message) {  
  
    }  
  
    @Override  
    public void onSubscribe(String channel, int subscribedChannels) {  
        // System.out.println("channel:" + channel + "is been subscribed:" + subscribedChannels);  
    }  
  
    @Override  
    public void onPUnsubscribe(String pattern, int subscribedChannels) {  
  
    }  
  
    @Override  
    public void onPSubscribe(String pattern, int subscribedChannels) {  
  
    }  
  
    @Override  
    public void onUnsubscribe(String channel, int subscribedChannels) {  
        System.out.println("channel:" + channel + "is been unsubscribed:" + subscribedChannels);  
    }  
	
	

}
