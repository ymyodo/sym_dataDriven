package com.sym.zookeeper.origin;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 原生的zookeeper驱动
 *
 * @author ym.shen
 * Created on 2020/4/10 18:32
 */
public class ZookeeperTest {

    @Test
    public void test0001() throws KeeperException, InterruptedException, IOException {
        Watcher watcher = event -> {
            System.out.println(event.getState());
            System.out.println(event.getType());
            System.out.println(event.getPath());
            System.out.println(event.getWrapper());
        };
        ZooKeeper zooKeeper = ZookeeperClientUtil.newZookeeper("127.0.0.1:2181", watcher);
        String s = zooKeeper.create("/test", "666".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("创建节点：" + s);
    }
}
