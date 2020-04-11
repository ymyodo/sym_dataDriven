package com.sym.zookeeper.curator;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 获取{@link org.apache.curator.framework.CuratorFramework}实例
 *
 * @author ym.shen
 * Created on 2020/4/10 17:12
 */
public class CuratorClientUtil {

    /**
     * 获取 zk 客户端
     */
    public static CuratorFramework getClient(){
        return getClient(null);
    }

    /**
     * 获取 zk 客户端
     * @param baseNameSpace zk基础路径, 创建的客户端的操作都会在这个目录下
     */
    public static CuratorFramework getClient(String baseNameSpace){

        // zk客户端与zk服务端的会话超时, 单位毫秒
        int sessionTimeOut = 60 * 1000;
        // zk客户端与zk服务端的连接超时, 单位毫秒
        int connectionTimeOut = 30 * 1000;
        // 重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        // 获取建造器
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString("127.0.0.1:2181")
                .sessionTimeoutMs(sessionTimeOut)
                .connectionTimeoutMs(connectionTimeOut)
                .retryPolicy(retryPolicy);
        if(StringUtils.isNoneBlank(baseNameSpace)){
            builder.namespace(baseNameSpace);
        }
        CuratorFramework framework = builder.build();
        framework.start();
        return framework;
    }
}
