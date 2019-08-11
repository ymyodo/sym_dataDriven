package com.sym.elasticsearch.jest;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.sort.Sort;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 使用Jest操作ES
 *
 * @Auther:
 * @Date: 2018-11-27 9:39
 */

public class JestUtil {

    // 日志记录
    private final static Logger LOGGER = LoggerFactory.getLogger(JestUtil.class);
    // 连接工厂
    private static JestClientUtil jestClientUtil = new JestClientUtil();

    /**
     * jest连接工厂
     */
    static class JestClientUtil{
        private static String defaultFile = "jest.properties";// 默认配置文件路径
        private static String defaultHost = "http://127.0.0.1:9200"; // 默认主机地址
        private static Map<String, JestClient> clientMap;// 保存连接客户端
        private static PropertiesConfiguration propertyConfig;// 用于读取配置文件

        static {
            clientMap = new ConcurrentHashMap<>();
            init();
            initClient(getHost());
        }

        /**
         * 使用 PropertiesConfiguration 加载配置文件
         */
        private static void init() {
            try {
                LOGGER.info("加载配置文件：" + defaultFile);
                propertyConfig = new PropertiesConfiguration();
                propertyConfig.setEncoding("UTF-8");
                propertyConfig.load(defaultFile);
            } catch (Exception e) {
                LOGGER.warn("加载配置文件 " + defaultFile + " 出错,原因：" + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * 获取配置文件内的主机地址
         *
         * @return
         */
        private static String[] getHost() {
            List<String> retList = new ArrayList<>();
            String cluster = propertyConfig.getString("cluster");
            if (StringUtils.isBlank(cluster)) {
                retList.add(defaultHost);
            } else {
                String[] hosts = cluster.split(",");
                for (String host : hosts) {
                    if (StringUtils.isNotBlank( host )) {
                        retList.add(host);
                    }
                }
            }
            String[] retArray = new String[retList.size()];
            LOGGER.info("获取到的主机地址为：" + retList);
            return retList.toArray(retArray);
        }


        /**
         * 初始化客户端连接
         *
         * @param hosts ES主机地址
         */
        private static void initClient(String... hosts) {
            JestClientFactory factory = new JestClientFactory();
            HttpClientConfig config;
            String prefix = "http://";
            for (String host : hosts) {
                // 保存主机地址即可
                int index = host.indexOf(":");
                if (index == -1) {
                    continue;
                }
                if (!clientMap.containsKey( host )) {
                    // 这边需要继续完善，以便完成额外配置
                    config = new HttpClientConfig.Builder(prefix + host).build();
                    // 保存key和client
                    clientMap.put(host.substring(0, index), factory.getObject());
                    LOGGER.info("添加新的ES客户端，IP = {}", host);
                }
            }
        }


        /**
         * 添加新的客户端
         * @param ips 要添加的主机地址
         */
        public void addClient(String...ips){
            initClient(ips);
        }

        /**
         * 获取指定IP客户端
         * @param ip
         * @return
         */
        public JestClient getClient(String ip){
            return clientMap.get(ip);
        }

        /**
         * 关闭客户端
         * @param ip
         */
        public void closeClient(String ip){
            JestClient client = getClient(ip);
            if (client == null) {
                LOGGER.info("未初始化ip=" + ip + "的客户端");
                return;
            }
            try {
                clientMap.remove(ip);
                client.close();
                LOGGER.info("成功关闭ip=" + ip + "的客户端");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 添加新的客户端
     *
     * @param ips
     */
    public static void addClient(String...ips) {
        jestClientUtil.addClient(ips);
    }


    /**
     * 获取客户端
     *
     * @param ip 主机地址
     * @return
     */
    public static JestClient getClient(String ip) {
        return jestClientUtil.getClient(ip);
    }


    /**
     * 关闭客户端
     *
     * @param ip
     */
    public static void CloseClient(String ip) {
        jestClientUtil.closeClient(ip);
    }


    /**
     * 指定主机添加无配置的索引
     *
     * @param host
     * @param indexName
     * @return
     */
    public static boolean createIndex(String host, String indexName) {
        return createIndex(host, indexName, null, null);
    }


    /**
     * 指定主机添加带配置的索引
     *
     * @param host
     * @param indexName
     * @param settings
     * @return
     */
    public static boolean createIndex(String host, String indexName, Map<String, Object> settings, Map<String, Object> mappings) {
        if (StringUtils.isBlank(indexName)) {
            throw new RuntimeException("索引名称不能为空");
        }
        CreateIndex.Builder builder = new CreateIndex.Builder(indexName);
        if (settings != null) {
            builder.settings(settings);
        }
        if (mappings != null) {
            builder.mappings(mappings);
        }
        CreateIndex createIndex = builder.build();
        JestClient client = getClient(host);
        if (client == null) {
            LOGGER.warn("客户端 " + host + " 未被初始化,无法添加索引");
            return false;
        } else {
            try {
                JestResult jestResult = client.execute(createIndex);
                return jestResult.isSucceeded();
            } catch (IOException e) {
                LOGGER.warn("添加索引失败，host=" + host + "，原因：" + e.getMessage());
                return false;
            }
        }
    }


    /**
     * 删除索引
     *
     * @param host
     * @param indexName
     * @return
     */
    public static boolean deleteIndex(String host, String indexName) {
        DeleteIndex build = new DeleteIndex.Builder(indexName).build();
        JestClient client = getClient(host);
        try {
            JestResult jestResult = client.execute(build);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 查询ES
     *
     * @param host  主机地址
     * @param query 查询语句
     * @return
     */
    public static SearchResult search(String host, String query) {
        return search(host, query, null, null, null);
    }


    /**
     * 查询ES
     *
     * @param host  主机地址
     * @param query 查询语句
     * @param index 查询索引
     * @return
     */
    public static SearchResult search(String host, String query, String index) {
        return search(host, query, index, null, null);
    }


    /**
     * 查询ES
     *
     * @param host  主机地址
     * @param query 查询语句
     * @param index 查询索引
     * @param type  类型
     * @return
     */
    public static SearchResult search(String host, String query, String index, String type) {
        return search(host, query, index, type, null);
    }


    /**
     * 查询ES
     *
     * @param query 查询语句
     * @param index 索引
     * @param type  类型
     * @param sorts 排序
     * @return
     */
    public static SearchResult search(String host, String query, String index, String type, Sort... sorts) {
        JestClient client = getClient(host);
        if (client == null) {
            LOGGER.warn("获取不到IP={}的客户端", host);
            return null;
        }
        Search.Builder builder = new Search.Builder(query);
        if (StringUtils.isNotBlank(index)) {
            builder.addIndex(index);
        }
        if (StringUtils.isNotBlank(type)) {
            builder.addType(type);
        }
        if (sorts != null && sorts.length > 0) {
            builder.addSort(Arrays.asList(sorts));
        }
        Search search = builder.build();
        try {
            return client.execute(search);
        } catch (IOException e) {
            LOGGER.error("查询失败，原因：{}", e.getMessage());
            return null;
        }
    }


    /**
     * 转换 SearchResult 为map
     *
     * @param searchResult
     * @return map包含total和rows
     */
    public static Map<String, Object> readSearchResultWithTotal(SearchResult searchResult) {
        // 返回值
        Map<String, Object> retMap = new HashMap<>();
        retMap.put("total", 0);
        retMap.put("rows", null);
        // 查询结果为空
        if (searchResult == null) {
            return retMap;
        }

        // json转换工具
        Type mapType = new TypeToken<Map<String, Object>>() {
        }.getType();
        Gson gson = new Gson();

        // 查询失败
        if (!searchResult.isSucceeded()) {
            String errorMessage = searchResult.getErrorMessage();
            Map<String, Object> map = gson.fromJson(errorMessage, mapType);
            LOGGER.warn("查询失败，原因：" + map.get("reason"));
            return retMap;
        }
        // 读取数据
        Type listType = new TypeToken<List<Map<String, Object>>>() {
        }.getType();
        long total = searchResult.getTotal();
        List<Map> sourceAsObjectList = searchResult.getSourceAsObjectList(Map.class, false);
        retMap.put("total", total);
        retMap.put("rows", sourceAsObjectList);
        return retMap;
    }


    /**
     * 添加单条数据到ES中
     *
     * @param host   ES节点地址
     * @param index  索引
     * @param type   类型
     * @param source 源数据
     * @return
     */
    public static boolean addOne(String host, String index, String type, Object source) {
        return addOne(host, index, type, source, null);
    }


    /**
     * 添加单条数据到ES中
     *
     * @param host
     * @param index
     * @param type
     * @param source
     * @param id
     * @return
     */
    public static boolean addOne(String host, String index, String type, Object source, String id) {
        if (StrUtil.isBlank(index) || StrUtil.isBlank(type)) {
            LOGGER.warn("索引和类型不能为空");
            return false;
        }
        if (BeanUtil.isEmpty(source)) {
            LOGGER.warn("源数据不能为空");
            return false;
        }
        JestClient client = getClient(host);
        if (BeanUtil.isEmpty(client)) {
            LOGGER.warn("找不到IP为 {} 的ES节点", host);
            return false;
        }
        Index.Builder builder = new Index.Builder(source);
        builder.index(index);
        builder.type(type);
        if (StrUtil.isNotBlank(id)) {
            builder.id(id);
        }
        try {
            client.execute(builder.build());
            return true;
        } catch (IOException e) {
            LOGGER.error("添加数据失败，原因：{}", e.getMessage());
            return false;
        }
    }


    /**
     * 批量添加数据
     *
     * @param host
     * @param defaultIndex
     * @param defaultType
     * @param ts
     * @return
     */
    public static <T> boolean addList(String host, String defaultIndex, String defaultType, T...ts) {
        if (ArrayUtil.isEmpty(ts)) {
            LOGGER.warn("对象数组为空");
            return false;
        }
        JestClient client = getClient(host);
        if (BeanUtil.isEmpty(client)) {
            LOGGER.warn("主机地址不存在：{}", host);
            return false;
        }
        Bulk.Builder builder = new Bulk.Builder();
        Stream<Index> stream = null;
        Class componentType = ts.getClass().getComponentType();

        if (componentType == Index.class) {
            stream = Arrays.stream(ts).map((a) -> {
                return (Index) a;
            });
        } else if( componentType == Index.Builder.class ){
            stream = Arrays.stream(ts).map((a) -> {
                Index.Builder builder1 = (Index.Builder) a;
                return builder1.build();
            });
        } else {
            if (StrUtil.isBlank(defaultIndex) || StrUtil.isBlank(defaultType)) {
                LOGGER.warn("必须指定Index和type");
                return false;
            }
            builder.defaultIndex(defaultIndex);
            builder.defaultType(defaultType);
            stream = Arrays.stream(ts).map((a) -> {
                Index.Builder builder1 = new Index.Builder(a);
                return builder1.build();
            });
        }
        builder.addAction(stream.collect(Collectors.toList()));
        try {
            client.execute(builder.build());
            return true;
        } catch (IOException e) {
            LOGGER.error("批量增加失败，原因：{}", e.getMessage());
            return false;
        }
    }


}
