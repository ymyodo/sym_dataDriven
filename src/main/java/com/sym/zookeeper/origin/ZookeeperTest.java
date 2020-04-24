package com.sym.zookeeper.origin;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * 原生的zookeeper驱动
 *
 * @author ym.shen
 * Created on 2020/4/10 18:32
 */
@Slf4j
public class ZookeeperTest {

    private String zookeeperHost = "127.0.0.1:2181";
    private ZooKeeper zooKeeper;

    @Before
    public void before() {
        zooKeeper = ZookeeperClientUtil.newZookeeper(zookeeperHost);
        log.info("zk客户端：{}", zookeeperHost);
    }

    /**
     * 同步的方式创建一个节点
     */
    @Test
    public void syncCreateNode() throws KeeperException, InterruptedException {
        /*
         * 同步创建一个节点会有4个参数, 但是原生驱动没办法递归创建父目录
         * 1.path：节点路径
         * 2.data[]：节点的初始内容
         * 3.acl：节点的ACL策略
         * 4.createMode：节点类型，有四种可选：持久、持久顺序、临时和临时顺序
         */
        String s = zooKeeper.create("/test", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        log.info("zookeeper返回信息：{}", s);
    }

    /**
     * 异步创建一个节点
     */
    @Test
    public void asyncCreateMode() throws BrokenBarrierException, InterruptedException {
        /*
         * 异步创建一个节点最全会有5个参数, 但是原生驱动没办法递归创建父目录
         * 1.path：节点路径
         * 2.data[]：节点的初始内容
         * 3.acl：节点的ACL策略
         * 4.createMode：节点类型，有四种可选：持久、持久顺序、临时和临时顺序
         * 5.cb：注册一个异步回调回调函数,
         * 6.ctx：传递一个对象, 在回调方法执行时候使用，一般是一个上下文对象
         */
        String context = "模拟上下文对象";
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        zooKeeper.create("/test", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
            /**
             * AsyncCallback提供了不同的回调接口, 在不同场景中使用
             * @param rc 响应码：0, 创建成功; -4, 服务端与客户端连接断开; -110, 指定节点已存在; -112, 会话过期
             * @param path 节点路径
             * @param ctx 接口传入的附加对象
             * @param name 该节点的完整路径
             */
            @SneakyThrows
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                log.info("异步创建节点成功, 返回码：{}, 创建路径：{}, 上下文对象：{}, 节点名称：{}", rc, path, ctx, name);
                cyclicBarrier.await();
            }
        }, context);
        cyclicBarrier.await();
    }

    /**
     * 同步删除一个节点
     */
    @Test
    public void syncDeleteNode() throws KeeperException, InterruptedException {
        String path = "/test";
        // 创建一个节点
        String s = zooKeeper.create(path, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        log.info("zookeeper服务端返回：{}", s);
        /*
         * 此方法只允许删除叶子节点, 当一个节点存在子节点, 必须先将它所有的子节点删除掉才能删除它, 有两个参数：
         * path：节点路径
         * version：节点版本, 0 或 -1 表示不管版本
         */
        zooKeeper.delete(path, 0);
    }

    /**
     * 异步删除一个节点
     */
    @Test
    public void asyncDeleteNode() throws KeeperException, InterruptedException, BrokenBarrierException {
        String path = "/test";
        // 创建一个节点
        String s = zooKeeper.create(path, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        log.info("zookeeper服务端返回：{}", s);
        /*
         * 此方法只允许删除叶子节点, 当一个节点存在子节点, 必须先将它所有的子节点删除掉才能删除它, 有两个参数：
         * path：节点路径
         * version：节点版本, 0 或 -1 表示不管版本
         * VoidCallback：删掉完毕回调
         * ctx：上面回调方法的附加对象
         */
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        Object ctx = "模拟全局对象";
        zooKeeper.delete(path, 0, new AsyncCallback.VoidCallback() {
            /**
             * @param rc 响应码：0, 创建成功; -4, 服务端与客户端连接断开; -110, 指定节点已存在; -112, 会话过期
             * @param path 节点路径
             * @param ctx 附加对象
             */
            @SneakyThrows
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if(rc == 0){
                    log.info("节点删除成功, 返回码：{}, 节点路径：{}, 附加对象：{}", rc, path, ctx);
                    cyclicBarrier.await();
                }else{
                    log.error("节点删除失败, 返回码：{}", rc);
                }
            }
        }, ctx);
        cyclicBarrier.await();
    }
}
