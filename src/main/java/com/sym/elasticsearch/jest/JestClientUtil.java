package com.sym.elasticsearch.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基于Jest的es客户端工具类
 *
 * Created by shenym on 2019/8/6 16:38.
 */
@Component
public class JestClientUtil implements InitializingBean {

    private final static Logger LOGGER = LoggerFactory.getLogger(JestClientUtil.class);

    private static final byte[] lock = new byte[0];

    private static List<String> urlList;

    /* 可以设计成单例，它接受批量的ES服务节点 */
    private static JestClient jestClient = null;

    @Autowired
    private Environment env;

    /**
     * 获取一个jest的客户端连接
     *
     * @return JestClient(在jest中 JestClient是一个单例)
     */
    public static JestClient getJestClient(){
        if( null == jestClient ){
            synchronized ( lock ){
                if( null == jestClient ){
                    JestClientFactory factory = new JestClientFactory();
                    // 后面有额外配置,直接在这边改
                    factory.setHttpClientConfig(new HttpClientConfig.Builder(urlList)
                            .multiThreaded(true)
                            .maxTotalConnection(20)
                            .build());
                    jestClient = factory.getObject();
                }
                return jestClient;
            }
        }else{
            return jestClient;
        }
    }


    /**
     * 将配置的ES节点地址(逗号隔开)转换为List
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        urlList = new ArrayList<>();
        String urls = env.getProperty("es.jest.common.urls");
        if(StringUtils.isBlank( urls )){
            LOGGER.info("未获取到任何ES节点地址配置,使用默认地址：127.0.0.1:9200");
            urlList.add("127.0.0.1:9200");
        }else{
            LOGGER.info("获取到ES节点地址配置：{}",urls);
            String[] urlArray = urls.split(",");
            Arrays.stream(urlArray).forEach(url->{
                if( !StringUtils.isBlank(url) ){
                    urlList.add(url);
                }
            });
        }
    }
}
