package com.sym.zookeeper.origin;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * zookeeper原生获取驱动是异步, 这里对其做同步处理
 *
 * @author ym.shen
 * Created on 2020/4/23 17:45
 */
@Slf4j
public class ZookeeperClientUtil {
    public static void main(String[] args) {
        String connectionString = "127.0.0.1:2181";
        long connectionTimeout = 10000;
        int sessionTimeout = 10000;
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //
            }
        };
        boolean canBeReadOnly = false;
        ZooKeeper zooKeeper = ZookeeperClientUtil.newZookeeper(connectionString, connectionTimeout, sessionTimeout, watcher, canBeReadOnly, -1, null);
        System.out.println(zooKeeper);
    }

    /*
     * 创建一个Zookeeper最全需要5个参数, 分别为：
     * connectString, 表示zk集群地址, 例如：192.168.1.1:2181,192.16.1.2:2181,192.168.1.3:2181
     * sessionTimeout, 表示与zk服务器的心跳超时时间, 单位毫秒
     * watcher, 监听器
     * canBeReadOnly, 表示此次会话是不是只读模式
     * sessionId和sessionPasswd, 表示会话的id和会话密码, 用来进行会话复用
     */

    public static ZooKeeper newZookeeper(String connectionString, long connectionTimeout, int sessionTimeout, Watcher watcher,
                                         boolean canBeReadOnly, long sessionId, String sessionPassword) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(1);
        ZooKeeper zookeeper;
        try {
            if (sessionId > 0 && StringUtils.isNoneBlank(sessionPassword)) {
                zookeeper = new ZooKeeper(connectionString, sessionTimeout, WatchProxy.getProxy(cyclicBarrier, watcher), sessionId, sessionPassword.getBytes(), canBeReadOnly);
            } else {
                zookeeper = new ZooKeeper(connectionString, sessionTimeout, WatchProxy.getProxy(cyclicBarrier, watcher), canBeReadOnly);
            }
            cyclicBarrier.await();
            return zookeeper;
        } catch (IOException e) {
            log.error("创建zookeeper失败：{}", e.getMessage());
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        return null;
    }


    private static class WatchProxy{

        private static ClassLoader classLoader  = WatchProxy.class.getClassLoader();
        private static Class[] classes = new Class[]{Watcher.class};

        public static Watcher getProxy(CyclicBarrier cyclicBarrier, Watcher watcher){
            return (Watcher)Proxy.newProxyInstance(classLoader, classes, new WatcherHandler(cyclicBarrier, watcher));
        }
    }

    @AllArgsConstructor
    @Slf4j
    private static class WatcherHandler implements InvocationHandler{

        private CyclicBarrier cyclicBarrier;
        private Watcher watcher;


        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            boolean isCall = false;
            if("process".equals(method.getName())){
                try {
                    WatchedEvent watchedEvent = (WatchedEvent)args[0];
                    if(watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected){
                        cyclicBarrier.await();
                        isCall = true;
                    }
                }finally {
                    if(!isCall){
                        cyclicBarrier.await();
                    }
                }
            }
            return method.invoke(watcher, args);
        }
    }
}
