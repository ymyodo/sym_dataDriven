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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * 原生的zookeeper驱动
 *
 * @author ym.shen
 * Created on 2020/4/10 18:32
 */
@Slf4j
public class ZookeeperTest {

    private ZooKeeper zooKeeper;

    private String zookeeperHost = "127.0.0.1:2181";


    @Before
    public void before() {
        Watcher defaultWatcher = event -> log.info("事件发生: {}", event);
        zooKeeper = ZookeeperClientUtil.newZookeeper(zookeeperHost, defaultWatcher);
        log.info("zk客户端：{}", zookeeperHost);
        try {
            zooKeeper.delete("/test", -1);
        } catch (Exception e) {
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
         * version：节点版本, -1 表示不管版本
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
         * version：节点版本, -1 表示不管版本
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
                if (rc == 0) {
                    log.info("节点删除成功, 返回码：{}, 节点路径：{}, 附加对象：{}", rc, path, ctx);
                    cyclicBarrier.await();
                } else {
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

    /**
     * 同步获取一个节点的数据
     */
    @Test
    public void syncGetData() throws KeeperException, InterruptedException {
        // 创建一个节点
        String path = "/test";
        zooKeeper.create(path, "shenyanming".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        /*
         * 获取节点数据：
         * patch: 指定数据节点的路径, 获取该节点的数据;
         * watcher: 注册监听器, 后面如果该节点的数据变更, zk服务端就会回调此方法, 可为null;
         * stat: 指定数据节点的节点状态信息, 在方法回调时, zk服务端会用一个新的stat替换它;
         * watch: 表明是否需要注册一个watcher, 若为true则会使用创建Zookeeper对象时使用的默认Watcher对象;
         * cb: 异步获取节点数据时使用, 为一个异步回调方法;
         * ctx: 异步回调时的附加对象, 一般用于传递上下文
         */
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.info("事件发生: {}, 版本号：{}", event, stat.getVersion());
            }
        }, stat);
        log.info("获取节点数据: {}", new String(data));
        // 设置节点数据
        zooKeeper.setData(path, "good job".getBytes(), 0);
        Thread.sleep(1000);
        // getData()注册的Watcher也是只会生效一次, 需要重复注册.
        zooKeeper.setData(path, "to be no.1".getBytes(), 1);
    }

    /**
     * 异步获取节点数据
     */
    @Test
    public void asyncGetData() throws KeeperException, InterruptedException, BrokenBarrierException {
        // 节点路径
        String path = "/test";
        // 节点数据
        byte[] bytes = "shenYanMing".getBytes();
        // 监听器
        Watcher watcher = new Watcher() {
            @SneakyThrows
            @Override
            public void process(WatchedEvent event) {
                log.info("事件发生: {}", event);
                // 需要重复注册, 否则此Watcher只会生效一次
                zooKeeper.getData(path, this, null);
            }
        };
        // 创建一个节点
        zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        // 获取节点的数据
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        zooKeeper.getData(path, watcher, new AsyncCallback.DataCallback() {
            @SneakyThrows
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                log.info("异步回调: rc: {}, path: {}, ctx: {}, stat: {}", rc, path, ctx, stat);
                cyclicBarrier.await();
            }
        }, "模拟上下文");
        cyclicBarrier.await();
        // 更新节点数据, 观察是否重复注册Watcher在每一次数据变更都能生效
        zooKeeper.setData(path, "update_".getBytes(), 0);
        Thread.sleep(1000);
        zooKeeper.setData(path, "update_11".getBytes(), 1);
    }

    /**
     * 同步修改节点数据
     */
    @Test
    public void syncSetData() throws KeeperException, InterruptedException {
        // 创建一个节点
        String path = "/test";
        byte[] data = "zookeeper".getBytes();
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        /*
         * 修改节点数据:
         * 1.path: 指定数据节点的路径, 更新该节点的数据;
         * 2.data[]: 使用该数据覆盖数据节点的原数据;
         * 3.version: 指定数据节点的版本号, 表示此次更新针对该数据版本进行的;
         * 4.cb: 注册一个异步回调函数;
         * 5.ctx: 用于传递上下文信息的对象
         */
        // zookeeper的版本号是从0开始的, 若给定值为-1, 则告诉zk服务端, 要根据该节点的最新版本来更新,
        Stat stat = zooKeeper.setData(path, "update_".getBytes(), -1);
        data = zooKeeper.getData(path, false, stat);
        log.info("获取的数据: {}, 版本号: {}", new String(data), stat.getVersion());

        Thread.sleep(1000);

        // 每一次更新zk都会讲节点的版本号加1(用的是CAS算法), 所以下一次更新就需要获取到指定的版本号, 它是通过上一次的返回值Stat得到的
        stat = zooKeeper.setData(path, "update_".getBytes(), stat.getVersion());
        data = zooKeeper.getData(path, false, stat);
        log.info("获取的数据: {}, 版本号: {}", new String(data), stat.getVersion());

        // 当然随时可以用 -1 来更新, 如果版本号不对, zk会抛出：KeeperException$BadVersionException
        stat = zooKeeper.setData(path, "update_".getBytes(), -1);
    }

    @Test
    public void asyncSetData() throws KeeperException, InterruptedException {
        // 创建一个节点
        String path = "/test";
        byte[] data = "zookeeper".getBytes();
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // 更新节点数据
        CountDownLatch countDownLatch = new CountDownLatch(1);
        zooKeeper.setData(path, "niu, niu".getBytes(), -1, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                log.info("异步回调: rc: {}, path: {}, ctx: {}, stat: {}, 版本号: {}", rc, path, ctx, stat, stat.getVersion());
                countDownLatch.countDown();
            }
        }, "模拟的上下文");
        countDownLatch.await();

        // 获取节点数据
        zooKeeper.getData(path, false, new Stat());
        log.info("获取节点数据: {}", new String(data));
    }

    /**
     * 同步判断节点是否存在
     */
    @Test
    public void syncExist() throws KeeperException, InterruptedException {
        String path = "/test";
        /*
         * 接口用来检测指定节点是否存在, 返回Stat对象, 如果节点不存在此返回值为null, 有5个参数可选：
         * 1.path: 指定节点的数据, 判断它是否存在;
         * 2.watcher: 注册一个监听器, 用来监听如下事件：节点创建、节点删除、节点更新. 但不会对子节点的各种变化进行通知;
         * 3.watch: 指定是否复用Zookeeper对象中默认的Watcher;
         * 4.cb: 注册一个异步回调函数;
         * 5.ctx: 传递一个上下文信息对象, 在异步回调方法中使用
         */
        Stat stat = zooKeeper.exists(path, new Watcher() {
            @SneakyThrows
            @Override
            public void process(WatchedEvent event) {
                log.info("事件发生: {}", event);
                zooKeeper.exists(event.getPath(), this);
            }
        });
        log.info("节点存在? {}", stat != null);

        // 创建节点
        zooKeeper.create(path, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // 更新节点数据
        zooKeeper.setData(path, "update".getBytes(), -1);

        Thread.sleep(1000);
    }

    /**
     * 异步判断节点是否存在
     */
    @Test
    public void asyncExist() throws KeeperException, InterruptedException, BrokenBarrierException {
        String path = "/test";
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.info("事件发生: {}", event);
            }
        }, new AsyncCallback.StatCallback() {
            @SneakyThrows
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                log.info("异步回调: rc: {}, path: {}, ctx: {}, stat: {}", rc, path, ctx, stat);
                cyclicBarrier.await();
            }
        }, "null");
        // 创建节点
        zooKeeper.create(path, "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        cyclicBarrier.await();
    }

    /**
     * zookeeper的权限控制
     */
    @Test
    public void acl() throws KeeperException, InterruptedException {
        String path = "/test";
        byte[] data = "data".getBytes();
        /*
         * 主要对Zookeeper客户端添加权限, 当一个节点增加了权限控制, 则客户端即Zookeeper对象, 必须包含这个
         * 权限才能访问并操作此节点; 但是对于删除操作, 比较特殊, 一个数据节点做了ACL权限, 即使用没有设置权限的Zookeeper客户端也可以删除它,
         * 但对于它旗下的子节点, 如果要删除它们, 就要求客户端必须拥有指定权限了
         *
         * 主要通过zookeeper.addAuthInfo()方法添加权限, 有两个参数：
         * 1.scheme: 权限控制模式, 分为world、auth、digest、ip和supper
         * 2.auth: 具体的权限信息
         */
        ZooKeeper localZookeeper = ZookeeperClientUtil.newZookeeper(zookeeperHost);
        // 赋予权限的客户端, 还有一个没有任何权限的客户端
        localZookeeper.addAuthInfo("digest", "role:add:bbc".getBytes());

        // 使用设置权限的客户端创建一个节点
        localZookeeper.create(path, data, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);

        // 使用带有权限的客户端访问数据
        data = localZookeeper.getData(path, false, null);
        log.info("节点数据: {}", new String(data));

        // 使用没有权限的客户端访问数据
        try{
            data = zooKeeper.getData(path, false, null);
        }catch(Exception e){
            log.error("异常: {}", e);
        }
    }
}
