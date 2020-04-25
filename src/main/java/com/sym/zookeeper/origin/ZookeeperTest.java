package com.sym.zookeeper.origin;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 原生的zookeeper驱动
 *
 * @author ym.shen
 * Created on 2020/4/10 18:32
 */
@Slf4j
public class ZookeeperTest {

    private ZooKeeper zooKeeper;

    @Before
    public void before() {
        String zookeeperHost = "127.0.0.1:2181";
        Watcher defaultWatcher = event -> log.info("事件发生: {}", event);
        zooKeeper = ZookeeperClientUtil.newZookeeper(zookeeperHost, defaultWatcher);
        log.info("zk客户端：{}", zookeeperHost);
        try {
            zooKeeper.delete("/test", -1);
        }catch (Exception e){
            log.error("ignore: {}", e.getMessage());
        }
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

    /**
     * 同步获取一个节点旗下的所有子节点
     */
    @Test
    public void syncGetChildren() throws KeeperException, InterruptedException {
        ArrayList<ACL> aclList = ZooDefs.Ids.OPEN_ACL_UNSAFE;

        // 创建父节点
        String path = "/test";
        String info = zooKeeper.create(path, null, aclList, CreateMode.PERSISTENT);
        log.info("创建父节点: {}", info);

        // 创建子节点
        String childPath0 = "/test/a1";
        info = zooKeeper.create(childPath0, "first".getBytes(), aclList, CreateMode.EPHEMERAL);
        log.info("创建子节点a1: {}", info);

        // 创建子节点2
        String childPath1 = "/test/a2";
        info = zooKeeper.create(childPath1, "second".getBytes(), aclList, CreateMode.EPHEMERAL);
        log.info("创建子节点a2: {}", info);

        /*
         * getChildren()有多个参数组合方式, 具体为：
         * path, 获取此节点旗下的所有子节点;
         * watcher, 此次获取完子节点后, 子节点列表发生变更后, zk服务端就会通过这个watcher向客户端发起通知, 此参数允许为null, 有个坑爹的东西就是这个监听器是一次性的, 一次回调后
         *          就失效了, 需要重复注册
         * watch, 表示是否需要注册一个watcher, 为false表示不需要注册监听器, 为true会使用创建 new Zookeeper() 时使用的那个watcher, 同上面一样这个也是回调一次也失效了;
         * cb, 异步回调函数, 用在异步获取子节点的场景;
         * ctx, 异步回调函数使用的附加参数;
         * stat, 指定数据节点的状态信息, 此对象会在方法执行过程中被zk服务端响应的新stat替换掉
         *
         */
        List<String> children = zooKeeper.getChildren(path, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                log.info("事件发生: {}", event);
                // 一次成功的回调事件后, 需要重新注册, 因为当前的这个watcher失效了
                // zooKeeper.getChildren(path, new Watcher() {});
            }
        });
        log.info("子节点路径：{}", children);

        // 创建新的子节点
        String childPath2 = "/test/a3";
        info = zooKeeper.create(childPath2, "third".getBytes(), aclList, CreateMode.EPHEMERAL);
        log.info("创建子节点a3: {}", info);

        // 删除旧的子节点, 此时的节点变更就没有回调了, 因为watcher只生效一次
        zooKeeper.delete(childPath1, -1);
    }

    /**
     * 异步获取子节点列表
     */
    @Test
    public void asyncGetChildren() throws KeeperException, InterruptedException, BrokenBarrierException {
        ArrayList<ACL> aclList = ZooDefs.Ids.OPEN_ACL_UNSAFE;

        // 创建父节点
        String path = "/test";
        String info = zooKeeper.create(path, null, aclList, CreateMode.PERSISTENT);
        log.info("创建父节点: {}", info);

        // 创建子节点
        String childPath0 = "/test/a1";
        info = zooKeeper.create(childPath0, "first".getBytes(), aclList, CreateMode.EPHEMERAL);
        log.info("创建子节点a1: {}", info);

        // 创建子节点2
        String childPath1 = "/test/a2";
        info = zooKeeper.create(childPath1, "second".getBytes(), aclList, CreateMode.EPHEMERAL);
        log.info("创建子节点a2: {}", info);

        //异步获取
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.info("子节点事件发生: {}", event);
            }
        }, new AsyncCallback.Children2Callback() {
            @SneakyThrows
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                log.info("异步回调: rc: {}, path: {}, ctx: {}, children: {}, stat: {}", rc, path, ctx, children, stat);
                cyclicBarrier.await();
            }
        }, "模拟上下文对象");
        cyclicBarrier.await();
        // 删除一个子节点
        zooKeeper.delete(childPath1, -1);
    }
}
