package com.sym.elasticsearch.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 获取一个Jest Client连接
 *
 * Created by shenYm on 2019/8/10.
 */
public class JestClientUtil {

    private static Map<String,JestClient> clientMap = new ConcurrentHashMap<>();

    /**
     * 通过配置类{@link HttpClientConfig}获取Jest Client连接
     * @param clientConfig
     * @return
     */
    public static JestClient getClient(HttpClientConfig clientConfig){
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        return factory.getObject();
    }


    public static JestClient getClient(String host){
        HttpClientConfig config = new HttpClientConfig.Builder(host).maxTotalConnection(20).multiThreaded(true).readTimeout(60).build();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(config);
        return factory.getObject();
    }


    /**
     * 根据主机地址获取要保存到Map中的key
     * @param key
     * @return
     */
    private static String getKey(String key){
        if(StringUtils.isBlank( key )) return "";
        key = key.replaceAll("http://","");
        if( !key.contains(":") ){
            key += ":9200";
        }
        return key;
    }



}
