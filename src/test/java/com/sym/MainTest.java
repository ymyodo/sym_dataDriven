package com.sym;

import com.sym.zookeeper.curator.example.CuratorBaseTemplate;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 本地测试类
 *
 * Created by 沈燕明 on 2019/8/4.
 */
public class MainTest {

    @Before
    public void init(){

    }

    /**
     * 测试Jest获取同一个es服务器的客户端是否是同一个，即是否为单例？
     */
    @Test
    public void testOne(){
        // 先把服务器上的ES开启来
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("127.0.0.1:8000").build();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        JestClient obj1 = factory.getObject();
        JestClient obj2 = factory.getObject();
        System.out.println(obj1==obj2); //false
        // 事实证明Jest没做单例处理
    }

    @Test
    public void zkTest() throws Exception {
        CuratorBaseTemplate template = new CuratorBaseTemplate();
        byte[] data = template.getData("/dubbo");
        System.out.println(data);
    }

    @Test
    public void test1() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 10000000, null);
        while(!zooKeeper.getState().isConnected()){
            Thread.sleep(1000);
        }
        zooKeeper.create("/test", "good".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void test() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 10000000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
        while(!zooKeeper.getState().isConnected()){
            Thread.sleep(1000);
        }
        byte[] bytes = zooKeeper.getData("/test", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath());
            }
        }, null);
        System.out.println(new String(bytes));
    }
}
