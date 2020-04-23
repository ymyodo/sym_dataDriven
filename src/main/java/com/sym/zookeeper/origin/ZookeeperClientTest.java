package com.sym.zookeeper.origin;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/**
 * 原生的zookeeper驱动
 *
 * @author ym.shen
 * Created on 2020/4/23 17:34
 */
public class ZookeeperClientTest {

    @Test
    public void test0001(){
        /*
         * 最全的有5个参数：
         * connectString, 表示zk集群地址, 例如：192.168.1.1:2181,192.16.1.2:2181,192.168.1.3:2181
         * sessionTimeout, 表示与zk服务器的超时时间, 单位毫秒
         * watcher, 监听器
         * canBeReadOnly, 表示此次会话是不是只读模式
         * sessionId和sessionPasswd, 表示会话的id和会话密码, 用来进行会话复用
         */
//        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, new Watcher() {
//            @Override
//            public void process(WatchedEvent event) {
//                if(event.getState() == Event.KeeperState.SyncConnected){
//
//                }
//            }
//        });
    }
}
