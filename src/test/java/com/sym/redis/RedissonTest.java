package com.sym.redis;

import com.sym.redis.redisson.RedissonClientUtil;
import org.junit.Test;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis的Java客户端-Redisson。
 * 首先要明确一点：Redisson并不是跟Jedis一样，仅仅是为了操作redis而做的一些命令封装，它在这种基础上，完善redis在分布式系统的运用
 * 例如分布式锁！所以最好在使用更高场景时再来使用Redisson，官方文档地址：https://github.com/redisson/redisson/wiki
 *
 * Redisson的API与redis原生命令的映射：
 * @see <a href="https://github.com/redisson/redisson/wiki/11.-Redis%E5%91%BD%E4%BB%A4%E5%92%8CRedisson%E5%AF%B9%E8%B1%A1%E5%8C%B9%E9%85%8D%E5%88%97%E8%A1%A8"></a>
 *
 * Created by shenym on 2019/9/23.
 */
public class RedissonTest {

    /**
     * 设置key、获取key、修改Key、删除Key
     */
    @Test
    public void crudTest(){
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


    /**
     * 获取分布式锁
     */
    @Test
    public void lockTest(){
        RedissonClient client = RedissonClientUtil.getRedissonClient();
        RLock lock = client.getLock("try-lock-test");
        lock.tryLock();
        lock.unlock();
    }


    /**
     * 获取布隆过滤器
     */
    @Test
    public void bloomTest(){
        RedissonClient client = RedissonClientUtil.getRedissonClient();
        RBloomFilter<Object> bloomFilter = client.getBloomFilter("test-bloom");
        bloomFilter.expire(10, TimeUnit.MINUTES);
        bloomFilter.tryInit(10000, 0.1);

        // 初始化, 将0-5554的值保存到布隆过滤器中
        for (int i = 0; i < 5555; i++) {
            bloomFilter.add(Integer.toString(i));
        }

        // 校验
        List<String> hitList = new ArrayList<>(10000); //命中集合
        List<String> unHitList = new ArrayList<>(10000); //未命中集合
        List<String> wrongExistList = new ArrayList<>(10000); //误判命中集合
        List<String> wrongNotExistList = new ArrayList<>(10000); //误判未命中集合
        for (int j = -100; j < 8000; j++) {
            String s = String.valueOf(j);
            if (bloomFilter.contains(s)) {
                // 命中
                hitList.add(s);
                if( j < 0 || j > 5554 ){
                    // 因为之前布隆过滤器初始化时, 只加入了0~5554的数据, 如果当前值不再这个范围内, 但是却被判断存在于布隆过滤器, 就是误判
                    wrongExistList.add(s);
                }
            } else {
                // 未命中
                unHitList.add(s);
                if( 0 <= j && j < 5555 ){
                    // 因为之前布隆过滤器初始化时, 只加入了0~5554的数据, 如果当前值属于这个范围内, 但是却被判断不存在于布隆过滤器, 就是误判
                    // PS：布隆过滤器只会误判存在, 不会误判不存在滴~~
                    wrongNotExistList.add(s);
                }
            }
        }

        // 打印结果
        System.out.println("命中数量：" + hitList.size());
        System.out.println(hitList);
        System.out.println("未命中数量：" + unHitList.size());
        System.out.println(unHitList);
        System.out.println("误判数量：" + (wrongExistList.size()+wrongNotExistList.size()));
        System.out.println("误判存在："+wrongExistList);
        System.out.println("误判不存在："+wrongNotExistList);
    }
}
