package com.sym.zookeeper.curator;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link CuratorFramework}的使用
 *
 * @author ym.shen
 * Created on 2020/4/10 17:24
 */
@Slf4j
public class CuratorTemplate {

    private static final Charset DEFAULT_CHARSET_UTF8 = StandardCharsets.UTF_8;

    private CuratorFramework curatorFramework;

    public CuratorTemplate() {
        curatorFramework = CuratorClientUtil.getClient();
    }

    public CuratorFramework getCurator() {
        return this.curatorFramework;
    }

    /**
     * 创建临时节点, 不携带任何数据
     *
     * @param path 节点路径
     */
    public void createEphemeralNode(String path) throws Exception {
        this.createNode(path, null, CreateMode.EPHEMERAL, false);
    }

    /**
     * 创建临时节点, 携带初始化数据
     *
     * @param path 节点路径
     * @param data 节点初始化数据
     */
    public void createEphemeraNode(String path, String data) throws Exception {
        this.createNode(path, data.getBytes(DEFAULT_CHARSET_UTF8), CreateMode.EPHEMERAL, false);
    }

    /**
     * 创建zk节点
     *
     * @param path                 节点路径
     * @param data                 节点数据
     * @param createMode           节点类型
     * @param isNeedToCreateParent 是否需要递归创建父节点
     * @throws Exception 创建异常
     */
    public void createNode(String path, byte[] data, CreateMode createMode, boolean isNeedToCreateParent) throws Exception {
        CreateBuilder createBuilder = curatorFramework.create();
        // 是否需要递归创建父节点
        if (isNeedToCreateParent) {
            createBuilder.creatingParentContainersIfNeeded();
        }
        /*
         * CreateMode 的不同含义：
         * 1.PERSISTENT：持久化
         * 2.PERSISTENT_SEQUENTIAL：持久化并且带序列号
         * 3.EPHEMERAL：临时节点
         * 4.EPHEMERAL_SEQUENTIAL：临时节点并且带序列化
         * 5.CONTAINER
         * 6.PERSISTENT_WITH_TTL：持久化节点, 但是该节点如果超过ttl时间没修改且没有任何子节点, 该节点就会被删除
         * 7.PERSISTENT_SEQUENTIAL_WITH_TTL：在上面基础上加上序列号
         */
        ACLBackgroundPathAndBytesable<String> bytesable = createBuilder.withMode(createMode);

        // 设置节点路径或数据
        String info;
        if (null != data) {
            info = bytesable.forPath(path, data);
        } else {
            info = bytesable.forPath(path);
        }
        log.info("[创建zk节点]{}", info);
    }

    /**
     * 删除叶子节点, 非叶子节点调用此方法会抛异常
     *
     * @param path 节点路径
     * @throws Exception 删除异常
     */
    public void deleteLeafNode(String path) throws Exception {
        this.deleteNode(path, -1, false);
    }

    /**
     * 在会话有效期间内, 保证删除zk节点, 防止由于网络原因导致的删除失败
     *
     * @param path 节点路径
     */
    public void deleteNodeGuaranteed(String path) throws Exception {
        curatorFramework.delete().guaranteed().forPath(path);
        log.info("[删除zk节点]path:{}", path);
    }

    /**
     * 删除zk节点
     *
     * @param path                        节点路径
     * @param version                     版本号, 不需要版本控制, 其值为-1
     * @param isNeedToDeleteChildrenNodes 递归删除子节点
     * @throws Exception 删除异常
     */
    public void deleteNode(String path, int version, boolean isNeedToDeleteChildrenNodes) throws Exception {
        DeleteBuilder deleteBuilder = curatorFramework.delete();
        // 按照版本删除
        if (version != -1) {
            deleteBuilder.withVersion(version);
        }
        // 是否需要连带删除子节点
        if (isNeedToDeleteChildrenNodes) {
            deleteBuilder.deletingChildrenIfNeeded();
        }
        // 执行删除
        deleteBuilder.forPath(path);
        log.info("[删除zk节点]path:{}", path);
    }


    /**
     * 获取指定节点的数据
     *
     * @param path 节点路径
     * @return 节点数据
     * @throws Exception 获取异常
     */
    public byte[] getData(String path) throws Exception {
        return curatorFramework.getData().forPath(path);
    }

    /**
     * 更新节点数据
     *
     * @param path 节点路径
     * @param data 待更新数据
     * @throws Exception 更新异常
     */
    public void updateData(String path, byte[] data) throws Exception {
        this.updateDate(path, data, -1);
    }

    /**
     * 更新节点数据
     *
     * @param path    节点路径
     * @param data    待更新数据
     * @param version 节点版本号
     * @throws Exception 更新异常
     */
    public void updateDate(String path, byte[] data, int version) throws Exception {
        SetDataBuilder setDataBuilder = curatorFramework.setData();
        // 强制版本更新
        if (version != -1) {
            setDataBuilder.withVersion(version);
        }
        if (null != data) {
            setDataBuilder.forPath(path, data);
        } else {
            setDataBuilder.forPath(path);
        }
    }

    /**
     * curator事务操作
     */
    public void transaction() throws Exception {
        CuratorTransaction curatorTransaction = curatorFramework.inTransaction();
        String path = "/sym/test";
        // curator可以开启一个事务, 每个操作用and()拼接, 这些操作要么一起执行要么一起不执行, 即原子性;
        // 最终通过commit()方法提交此事务即可
        curatorTransaction
                .check().forPath(path)
                .and()
                .create().forPath(path, "节点不存在才创建".getBytes(DEFAULT_CHARSET_UTF8))
                .and()
                .commit();
    }

    /**
     * curator的异步操作
     */
    public void asyncCreateNode(String path, byte[] data) throws Exception {
        // 异步操作可以指定一个Executor执行器, 若不指定, 会使用 curator 的默认 EventThread 去处理
        ExecutorService executorService = Executors.newCachedThreadPool();

        // 通过 inBackground()方法可以指定异步处理的回调器、上下文对象、执行器.
        // 其中回调器会在处理完成后调用, 并传递 curator客户端 和 相应的处理事件
        // 上下文对象可以在回调器中使用到, 通过 event.getContext()方法获取
        curatorFramework.create().withMode(CreateMode.EPHEMERAL)
                .inBackground((client, event) -> {
                    /*
                     * 0：OK，即调用成功
                     * -4：ConnectionLoss，即客户端与服务端断开连接
                     * -110：NodeExists，即节点已经存在
                     * -112：SessionExpired，即会话过期
                     */
                    int resultCode = event.getResultCode();
                    log.info("[异步创建节点]path:{}, code:{}",path, resultCode);

                }, this, executorService)
                .forPath(path, data);
    }
}
