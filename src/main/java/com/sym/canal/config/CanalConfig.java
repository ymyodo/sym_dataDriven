package com.sym.canal.config;

import lombok.Builder;
import lombok.Data;

/**
 * canal客户端配置类
 *
 * @author shenyanming
 * Create on 2021/07/12 17:47
 */
@Data
@Builder
public class CanalConfig {

    /**
     * canal server host
     */
    private String host;

    /**
     * canal server post
     */
    private Integer post;

    /**
     * canal server username
     */
    private String username;

    /**
     * canal server password
     */
    private String password;

    /**
     *
     */
    private String destination;

    /**
     * 订阅信息
     */
    private String filter;

    /**
     * 最大拉取条数
     */
    private int maxBatchSize;

    public static CanalConfig defaultConfig() {
        return CanalConfig.builder()
                .host("127.0.0.1")
                .post(11111)
                .username("")
                .password("")
                .destination("example")
                .maxBatchSize(1000)
                .filter(".*\\..*")
                .build();
    }
}
