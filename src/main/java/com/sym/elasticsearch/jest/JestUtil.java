package com.sym.elasticsearch.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.sort.Sort;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.PutMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * jest查询工具类
 * <p>
 * Created by shenym on 2019/8/6 16:59.
 */
public class JestUtil {

    private final static Logger LOGGER = LoggerFactory.getLogger(JestUtil.class);
    private final static String TOTAL_KEY = "total";
    private final static String ROWS_KEY = "rows";
    private final static String DEFAULT_TYPE = "_doc";

    /**
     * 新增一个索引
     *
     * @param indexName 索引名称
     * @param settings  索引配置
     * @return true-创建成功，false-创建失败
     */
    public static boolean createIndex(String indexName, Map<String, Object> settings) {
        if (StringUtils.isEmpty(indexName)) {
            LOGGER.warn("索引名称不能为空");
            return false;
        }
        CreateIndex.Builder builder = new CreateIndex.Builder(indexName);
        if (settings != null) {
            builder.settings(settings);
        }
        CreateIndex createIndex = builder.build();
        JestClient client = JestClientUtil.getJestClient();
        if (client == null) {
            LOGGER.warn("无法获取到JestClient客户端");
            return false;
        }
        try {
            JestResult jestResult = client.execute(createIndex);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            LOGGER.warn("添加索引{}失败,原因：{}", indexName, e.getMessage());
            return false;
        }
    }

    /**
     * 指定索引创建一个type
     *
     * @param indexName   索引名称
     * @param typeName    类型名称
     * @param mappingJson json串,形如:"{ \"my_type\" : { \"properties\" : { \"message\" : {\"type\" : \"string\", \"store\" : \"yes\"} } } }"
     * @return
     */
    public static boolean putMapping(String indexName, String typeName, String mappingJson) {
        if (org.apache.commons.lang3.StringUtils.isBlank(indexName)) {
            LOGGER.warn("索引名称不能为空");
            return false;
        }
        if (org.apache.commons.lang3.StringUtils.isBlank(mappingJson)) {
            LOGGER.warn("mapping映射不能为空");
            return false;
        }
        if( StringUtils.isEmpty(typeName) ) typeName = DEFAULT_TYPE;
        PutMapping putMapping = new PutMapping.Builder(indexName, typeName, mappingJson).build();
        try {
            JestResult jestResult = JestClientUtil.getJestClient().execute(putMapping);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            LOGGER.warn("索引{},添加类型{}失败,原因：{}", indexName, typeName, e.getMessage());
            return false;
        }
    }


    /**
     * 删除索引
     *
     * @param indexName 索引名称
     * @return true-删除成功
     */
    public static boolean deleteIndex(String indexName) {
        if (org.apache.commons.lang3.StringUtils.isBlank(indexName)) {
            LOGGER.warn("索引名称不能为空");
            return false;
        }
        DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).build();
        try {
            JestResult jestResult = JestClientUtil.getJestClient().execute(deleteIndex);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            LOGGER.error("删除索引{}失败，原因：{}", indexName, e.getMessage());
            return false;
        }
    }

    /**
     * 添加单条数据到ES
     *
     * @param index  索引
     * @param type   类型
     * @param source 源数据
     * @return
     */
    public static boolean addOne(String index, String type, Object source) {
        return addOne(index, type, source, null);
    }


    /**
     * 添加单条数据到ES中
     *
     * @param index  索引
     * @param type   类型
     * @param source 数据
     * @param _id    ES元数据
     * @return
     */
    public static boolean addOne(String index, String type, Object source, String _id) {
        if (org.apache.commons.lang3.StringUtils.isBlank(index)) {
            LOGGER.info("索引名称不能为空");
            return false;
        }
        if ( null != source ) {
            LOGGER.info("原数据不能为空");
            return false;
        }
        if( StringUtils.isEmpty(type) ) type = DEFAULT_TYPE;
        Index.Builder builder = new Index.Builder(source);
        builder.index(index);
        builder.type(type);
        if (!StringUtils.isEmpty(_id)) {
            builder.id(_id);
        }
        return addOne(builder.build());
    }


    /**
     * 添加单条数据到ES中
     *
     * @param source 数据
     * @return
     */
    public static boolean addOne(Index source) {
        if (null == source) {
            LOGGER.warn("原数据source不能为空");
            return false;
        }
        try {
            JestClientUtil.getJestClient().execute(source);
            return true;
        } catch (IOException e) {
            LOGGER.error("添加数据失败，原因：{}", e.getMessage());
            return false;
        }
    }


    /**
     * 批量添加数据
     *
     * @param index 索引
     * @param type  类型
     * @param list  原数据集
     * @return
     */
    public static <T> boolean addList(String index, String type, List<T> list) {
        if (org.apache.commons.lang3.StringUtils.isBlank(index)) {
            LOGGER.warn("索引名称不能为空");
            return false;
        }
        if (null == list || list.size() == 0) {
            LOGGER.warn("数据集合为空");
            return false;
        }
        if( StringUtils.isEmpty(type) ) type = DEFAULT_TYPE;
        final String typeName = type;
        List<Index> indices = list.stream().map((source -> {
            return new Index.Builder(source).index(index).type(typeName).build();
        })).collect(Collectors.toList());
        return addList(indices);
    }


    /**
     * 批量添加数据
     *
     * @param list {@link Index}数据集合
     * @return
     */
    public static boolean addList(List<Index> list) {
        if (null == list || list.size() == 0) {
            LOGGER.warn("数据集合为空");
            return false;
        }
        Bulk.Builder builder = new Bulk.Builder();
        builder.addAction(list);
        try {
            JestClientUtil.getJestClient().execute(builder.build());
            return true;
        } catch (IOException e) {
            LOGGER.error("批量增加失败，原因：{}", e.getMessage());
            return false;
        }
    }


    /**
     * 查询ES
     *
     * @param query 查询语句
     * @return
     */
    public static SearchResult search(String query) {
        return search(null, null, query, null);
    }


    /**
     * 查询ES
     *
     * @param index 查询索引
     * @param type  类型
     * @param query 查询语句
     * @return
     */
    public static SearchResult search(String index, String type, String query) {
        return search(index, type, query, null);
    }


    /**
     * 查询ES
     *
     * @param index 索引
     * @param type  类型
     * @param query 查询语句
     * @param sorts 排序
     * @return
     */
    public static SearchResult search(String index, String type, String query, List<Sort> sorts) {
        Search.Builder builder = new Search.Builder(query);
        if (!StringUtils.isEmpty(index)) {
            builder.addIndex(index);
        }
        if (!StringUtils.isEmpty(type)) {
            builder.addType(type);
        }
        if (sorts != null && sorts.size() > 0) {
            builder.addSort(sorts);
        }
        Search search = builder.build();
        try {
            return JestClientUtil.getJestClient().execute(search);
        } catch (IOException e) {
            LOGGER.error("查询失败，原因：{}", e.getMessage());
            return null;
        }
    }


    /**
     * 解析SearchResult对象，并且封装总数total和数据row
     *
     * @param searchResult
     * @param addEsMetadataFields
     * @return map包含total和rows
     */
    public static Map<String, Object> parseResultWithTotalAndRow(SearchResult searchResult, boolean addEsMetadataFields) {
        // 返回值
        Map<String, Object> retMap = new HashMap<>();
        retMap.put("total", 0);
        retMap.put("rows", null);
        // 查询结果为空
        if (searchResult == null) return retMap;
        // 查询失败
        if (!searchResult.isSucceeded()) {
            LOGGER.warn("查询失败，原因：{}", searchResult.getErrorMessage());
            return retMap;
        }
        // 读取数据
        long total = searchResult.getTotal();
        List<Map> sourceAsObjectList = searchResult.getSourceAsObjectList(Map.class, addEsMetadataFields);
        retMap.put(TOTAL_KEY, total);
        retMap.put(ROWS_KEY, sourceAsObjectList);
        return retMap;
    }


    /**
     * 解析SearchResult对象，只获取ES元数据
     *
     * @param searchResult
     * @param addEsMetadataFields
     * @return 只包含原数据
     */
    public static List<Map> parseResultOnlySource(SearchResult searchResult, boolean addEsMetadataFields) {
        if (null == searchResult || !searchResult.isSucceeded()) return Collections.emptyList();
        return searchResult.getSourceAsObjectList(Map.class, addEsMetadataFields);
    }


}
