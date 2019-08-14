package com.sym.elasticsearch.bboss;

import org.frameworkset.elasticsearch.entity.MapRestResponse;
import org.frameworkset.elasticsearch.entity.MapSearchHit;
import org.frameworkset.elasticsearch.entity.MapSearchHits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bboss操作工具类
 *
 * Created by shenym on 2019/8/8 8:25.
 */
public class BbossUtil {

    public final static String TOTAL_KEY = "total";
    public final static String DATA_KEY = "data";


    /**
     * 解析Bboss的 MapRestResponse
     * @param response 访问es的返回变量
     * @param getMetadata 是否获取ES的元数据
     * @return
     */
    public static Map<String,Object> readMapRestResponse(MapRestResponse response,boolean getMetadata){
        if (response == null || response.getSearchHits() == null) {
            return null;
        }
        MapSearchHits searchHits = response.getSearchHits();
        List<MapSearchHit> hits = searchHits.getHits();
        if (hits == null || hits.size() == 0) {
            return null;
        }
        // 返回值
        Map<String, Object> retMap = new HashMap<>();
        // 存储具体数据的list
        List<Map<String, Object>> dataList = new ArrayList<>(hits.size());
        // 遍历hits,获取所有ES返回的结果
        Map<String, Object> map;
        for (MapSearchHit mapSearchHit : hits) {
            map = mapSearchHit.getSource();
            if( getMetadata ){
                map.put("_id", mapSearchHit.getId());
                map.put("_index", mapSearchHit.getIndex());
                map.put("_type", mapSearchHit.getType());
                map.put("_version", mapSearchHit.getVersion());
                map.put("_routing", mapSearchHit.getRouting());
            }
            dataList.add(map);
        }
        // 总的数据量
        retMap.put(TOTAL_KEY, searchHits.getTotal());
        // 具体的数据值
        retMap.put(DATA_KEY, dataList);
        return retMap;
    }


    /**
     * 仅获取ES原数据
     * @param response
     * @return
     */
    public static List<Map<String,Object>> getSourceDataList(MapRestResponse response){
        if (response == null || response.getSearchHits() == null) {
            return null;
        }
        MapSearchHits searchHits = response.getSearchHits();
        List<MapSearchHit> hits = searchHits.getHits();
        if (hits == null || hits.size() == 0) {
            return null;
        }
        List<Map<String,Object>> retList = new ArrayList<>(hits.size());
        hits.forEach(hit-> retList.add(hit.getSource()));
        return retList;
    }
}
