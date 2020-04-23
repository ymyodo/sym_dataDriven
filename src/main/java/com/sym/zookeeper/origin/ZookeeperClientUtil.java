package com.sym.zookeeper.origin;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
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

    /**
     * 默认连接超时1分钟
     */
    private final static long DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;

    /**
     * 默认心跳会话超时 15s
     */
    private final static int DEFAULT_SESSION_TIMEOUT = 15 * 1000;

    public static ZooKeeper newZookeeper(String connectionString){
        return newZookeeper(connectionString, DEFAULT_CONNECTION_TIMEOUT,
                DEFAULT_SESSION_TIMEOUT, null, false,-1, null);
    }

    public static ZooKeeper newZookeeper(String connectionString, long connectionTimeout){
        return newZookeeper(connectionString, connectionTimeout,
                DEFAULT_SESSION_TIMEOUT, null, false, -1, null);
    }

    public static ZooKeeper newZookeeper(String connectionString, Watcher watcher){
        return newZookeeper(connectionString, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SESSION_TIMEOUT,
                watcher, false, -1, null);
    }

    public static ZooKeeper newZookeeper(String connectionString,
                                         long connectionTimeout,
                                         int sessionTimeout,
                                         Watcher watcher,
                                         boolean canBeReadOnly,
                                         long sessionId, String sessionPassword) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zookeeper;
        try {
            Watcher watcherProxy = WatchProxy.getProxy(countDownLatch, watcher);
            if (sessionId > 0 && StringUtils.isNoneBlank(sessionPassword)) {
                /*
                 * 创建一个Zookeeper最全需要5个参数, 分别为：
                 * connectString, 表示zk集群地址, 例如：192.168.1.1:2181,192.16.1.2:2181,192.168.1.3:2181
                 * sessionTimeout, 表示与zk服务器的心跳超时时间, 单位毫秒
                 * watcher, 监听器
                 * sessionId和sessionPasswd, 表示会话的id和会话密码, 可以用于复用会话
                 * canBeReadOnly, 标志此次会话是不是只读模式
                 */
                zookeeper = new ZooKeeper(connectionString, sessionTimeout, watcherProxy, sessionId, sessionPassword.getBytes(), canBeReadOnly);
            } else {
                zookeeper = new ZooKeeper(connectionString, sessionTimeout, watcherProxy, canBeReadOnly);
            }
            countDownLatch.await(connectionTimeout, TimeUnit.MILLISECONDS);
            if(zookeeper.getState() != ZooKeeper.States.CONNECTED){
                throw new TimeoutException("连接zookeeper超时");
            }
            return zookeeper;
        } catch (IOException | InterruptedException | TimeoutException e) {
            log.error("创建zookeeper失败：{}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * 监听器代理期
     */
    private static class WatchProxy {

        private static ClassLoader classLoader = WatchProxy.class.getClassLoader();
        private static Class[] classes = new Class[]{Watcher.class};
        /**
         * 默认监听器
         */
        private static Watcher defaultWatcher = event -> {
            // do nothing
        };

        /**
         * 创建代理对象
         * @param countDownLatch 同步工具
         * @param watcher 监听器
         */
        public static Watcher getProxy(CountDownLatch countDownLatch, Watcher watcher) {
            InvocationHandler invocationHandler;
            if(null == watcher){
                invocationHandler = new WatcherHandler(countDownLatch, defaultWatcher, true);
            }else{
                invocationHandler = new WatcherHandler(countDownLatch, watcher, false);
            }
            return (Watcher) Proxy.newProxyInstance(classLoader, classes, invocationHandler);
        }


        /**
         * 代理类处理逻辑
         */
        @Slf4j
        private static class WatcherHandler implements InvocationHandler {

            public WatcherHandler(CountDownLatch countDownLatch, Watcher watcher, boolean isIgnoreToInvoke){
                this.countDownLatch = countDownLatch;
                this.watcher = watcher;
                this.isIgnoreToInvoke = isIgnoreToInvoke;
                this.isInit = false;
            }

            private CountDownLatch countDownLatch;
            private Watcher watcher;
            private boolean isInit;
            private boolean isIgnoreToInvoke;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if("process".equals(method.getName()) &&
                        ((WatchedEvent)args[0]).getState() == Watcher.Event.KeeperState.SyncConnected){
                    if(!isInit){
                        countDownLatch.countDown();
                        countDownLatch = null;
                        isInit = true;
                    }else{
                        if(!isIgnoreToInvoke){
                            return method.invoke(watcher, args);
                        }
                    }
                }else{
                    // Object类的其它方法走这里
                    return method.invoke(watcher, args);
                }
                return null;
            }
        }
    }
}
