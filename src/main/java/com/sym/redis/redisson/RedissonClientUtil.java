package com.sym.redis.redisson;

import org.junit.Test;
import org.redisson.config.Config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * 获取 Redisson 客户端的工具类
 *
 * Created by 沈燕明 on 2019/10/14 22:09.
 */
public class RedissonClientUtil {

    @Test
    public void test() throws IOException {
        RedissonClientUtil.getSingleNodeConfig();
        RedissonClientUtil.getClusterConfig();
    }

    /**
     * 通过yaml文件获取单节点配置
     * @return
     * @throws IOException
     */
    public static Config getSingleNodeConfig() throws IOException {
        InputStream resourceAsStream = RedissonClientUtil.class.getClassLoader().getResourceAsStream("redisson/singleConfig.yaml");
        Config config = Config.fromYAML(resourceAsStream);
        System.out.println(config.toJSON());
        return config;
    }

    /**
     * 通过JSON文件获取集群配置
     * @return
     * @throws IOException
     */
    public static Config getClusterConfig() throws IOException {
        InputStream resourceAsStream = RedissonClientUtil.class.getClassLoader().getResourceAsStream("redisson/clusterConfig.json");
        Config config = Config.fromJSON(resourceAsStream);
        System.out.println(config.toJSON());
        return config;
    }

}
