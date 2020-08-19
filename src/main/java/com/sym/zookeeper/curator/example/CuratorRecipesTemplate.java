package com.sym.zookeeper.curator.example;

import com.sym.zookeeper.curator.CuratorClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.locks.*;
import org.apache.curator.framework.recipes.queue.*;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;


/**
 * {@link CuratorFramework}的高级特性用例
 *
 * @author ym.shen
 * Created on 2020/4/11 10:42
 */
@Slf4j
public class CuratorRecipesTemplate {

    private CuratorFramework curator;

    @Before
    public void before() {
        curator = CuratorClientUtil.getClient();
    }

    /**
     * 监听数据节点自身的变化
     */
    @Test
    public void nodeCache() throws Exception {
        String path = "/test";
        /*
         * 创建一个NodeCache最多需要3个参数：
         * 1.client: 表示 CuratorFramework 客户端
         * 2.path: 要监听的数据节点路径
         * 3.dataIsCompressed: 为true时在NodeCache第一次启动就会从Zookeeper服务端拉取节点数据保存到Cache中
         */
        NodeCache nodeCache = new NodeCache(curator, path, false);
        // 向其添加一个监听器, 在数据节点变更时回调
        // 1.若节点后创建, 可以在节点被创建后监听到;
        // 2.若节点数据被修改, 也可以监听到;
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() {
                ChildData currentData = nodeCache.getCurrentData();
                if (null == currentData) {
                    log.info("节点删除");
                } else {
                    log.info("节点数据变更, 路径: {}, 数据: {}, 节点状态: {}", currentData.getPath(),
                            new String(currentData.getData()), currentData.getStat());
                }
            }
        });
        // 启动监听
        nodeCache.start(true);

        // 创建节点
        curator.create().forPath(path, "lol".getBytes());

        // 修改节点数据
        curator.setData().forPath(path, "mhxy".getBytes());
        Thread.sleep(1000);

        // 删除节点
        curator.delete().forPath(path);

        // 线程休眠一会儿, 等待回调
        Thread.sleep(2000);
    }

    /**
     * 监听数据节点的子节点变化 孙子
     */
    @Test
    public void pathChildrenCache() throws Exception {
        String path = "/test";
        String childrenPath = "/test/a1";
        String grandsonPath = "/test/a1/b1";
        /*
         * PathChildrenCache构造方法参数说明：
         * 1.client: Curator客户端实例;
         * 2.path: 要监听的数据节点的路径;
         * 3.dataIsCompressed: 是否进行数据压缩, 即 pathChildrenCache 启动时就会去zookeeper拉取节点数据进行缓存
         * 4.cacheData: 用来配置是否把节点内容缓存起来, 若为true, 在收到子节点列表变更的同时也会获取到节点的数据内容
         * 5.threadFactory和executorService: 构建线程池, 异步处理通知
         */
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curator, path, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            /*
             * 当指定节点的子节点发生变化时, 就会回调该方法, PathChildrenCacheEvent 主要定义了常用的3个事件：
             * CHILD_ADDED(新增子节点)、CHILD_UPDATED(子节点数据变更)、CHILD_REMOVED(子节点删除)
             * 注意一点：PathChildrenCacheEvent无法监听二级子节点, 例如它监听“/test”, 可以收到“/test/a1”节点的信息, 但是无法监听到"/test/a1/bb"的二级节点信息
             */
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                ChildData childData = event.getData();
                switch (event.getType()){
                    case CHILD_ADDED:
                        log.info("添加子节点, {}", childData);
                        break;
                    case CHILD_UPDATED:
                        log.info("修改子节点, {}", childData);
                        break;
                    case CHILD_REMOVED:
                        log.info("移除子节点, {}", childData);
                        break;
                }
            }
        });
        /*
         * StartMode表示初始化方式：
         * 1.POST_INITIALIZED_EVENT：异步初始化。初始化后会触发事件
         * 2.NORMAL：异步初始化
         * 3.BUILD_INITIAL_CACHE：同步初始化
         */
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        // 创建子节点, 创建完需要休眠一会, PathChildrenCache 有做回调次数限制, 过于频繁会导致修改子节点这一步没有回调
        curator.create().creatingParentsIfNeeded().forPath(childrenPath, "abc".getBytes());
        Thread.sleep(1000);

        // 修改子节点
        curator.setData().forPath(childrenPath, "test".getBytes());
        Thread.sleep(1000);

        // 创建二级子节点, 观察是否有日志
        curator.create().creatingParentsIfNeeded().forPath(grandsonPath);

        // 删除节点
        curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);

        // 线程休眠
        Thread.sleep(5000);
    }

    /**
     * master选举
     */
    @Test
    public void leaderSelector(){
        String path = "/test";
        /*
         * Curator 有两种leader选举的方式, 分别是LeaderSelector和LeaderLatch.
         * 前者是所有存活的客户端不间断的轮流做Leader;
         * 后者是一旦选举出Leader, 除非有客户端挂掉重新触发选举，否则不会退出leader
         */
        LeaderSelector leaderSelector = new LeaderSelector(curator, path, new LeaderSelectorListener() {
            /**
             * 连接状态改变时回调
             */
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {

            }

            /**
             * 当外部的实例变成“leader”时回调此方法, 注意一旦此方法返回, 则意味着退出leader角色, 重新开始选举
             */
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {

            }
        });
    }

    /**
     * 可重入共享锁
     */
    @Test
    public void interProcessMutex(){
        String path = "/test";
        InterProcessMutex interProcessMutex = new InterProcessMutex(curator, path);
    }

    /**
     * 不可重入共享锁
     */
    @Test
    public void interProcessSemaphoreMutex(){
        String path = "/test";
        InterProcessSemaphoreMutex interProcessSemaphoreMutex = new InterProcessSemaphoreMutex(curator, path);
    }

    /**
     * 可重入读写锁
     */
    @Test
    public void interProcessReadWriteLock(){
        String path = "/test";
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(curator, path);
    }

    /**
     * 信号量
     */
    public void InterProcessSemaphoreV2(){
        String path = "/test";
        InterProcessSemaphoreV2 interProcessSemaphoreV2 = new InterProcessSemaphoreV2(curator, path, 2);
    }

    /**
     * 组锁实现, 一个锁的获取和释放, 都会传递相同操作给它包含的其它锁
     */
    @Test
    public void interProcessMultiLock(){
        String path = "/test";
        // 子锁
        InterProcessLock lock1 = new InterProcessMutex(curator, path);
        InterProcessLock lock2 = new InterProcessSemaphoreMutex(curator, path);

        // 组锁
        InterProcessMultiLock lock = new InterProcessMultiLock(Arrays.asList(lock1, lock2));
    }

    /**
     * 分布式int计数器
     */
    @Test
    public void SharedCount(){
        SharedCount sharedCount = new SharedCount(curator, "/test",1);
        //  可以为它增加一个SharedCountListener，当计数器改变时此Listener可以监听到改变的事件，
        //  而SharedCountReader可以读取到最新的值， 包括字面值和带版本信息的值VersionedValue
    }

    /**
     * 分布式long计数器
     */
    @Test
    public void distributedAtomicLong(){
        DistributedAtomicLong distributedAtomicLong = new DistributedAtomicLong(curator, "/test", new RetryNTimes(10, 10));
    }

    /**
     * 分布式队列
     */
    @Test
    public void distributedQueue(){
        // 分布式队列表接口, 有多种实现：普通队列、带id的队列、优先级队列、延迟队列
        QueueBase<String> queue;

        // 队列消费
        QueueConsumer<String> queueConsumer = new QueueConsumer<String>() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {

            }
            @Override
            public void consumeMessage(String message) throws Exception {

            }
        };

        // 队列序列化
        QueueSerializer<String> queueSerializer = new QueueSerializer<String>() {
            @Override
            public byte[] serialize(String item) {
                return new byte[0];
            }

            @Override
            public String deserialize(byte[] bytes) {
                return null;
            }
        };

        // 队列构造器
        QueueBuilder<String> queueBuilder = QueueBuilder.builder(curator, queueConsumer, queueSerializer, "/test");

        // 创建普通队列
        queue = queueBuilder.buildQueue();
        // 创建带id的队列
        queue = queueBuilder.buildIdQueue();
        // 创建延迟队列
        queue = queueBuilder.buildDelayQueue();
        // 创建优先级队列
        queue = queueBuilder.buildPriorityQueue(1);
    }

    /**
     * 分布式屏障
     */
    @Test
    public void distributedBarrier(){
        DistributedBarrier distributedBarrier = new DistributedBarrier(curator, "/test");
    }
}
