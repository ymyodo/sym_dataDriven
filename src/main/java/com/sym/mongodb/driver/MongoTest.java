package com.sym.mongodb.driver;
// 使用静态导入，省去写"Filters."

import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;

/**
 * 测试MongoUtil操作类
 *
 * @author user
 */
public class MongoTest {

    public static void main(String[] args) {
        MongoTest m = new MongoTest();
        // m.TestInsertOne();
        // m.findOne();
        // m.findMany();
        // m.updateOne();
    }

    /* 测试新增一个文档 */
    public void TestInsertOne() {
        // "stuId" : 700, "name" : "stu_7", "age" : 21, "sex" : "g", "score" :
        // 64, "like" : [ "Chinese", "English", "Math" ] }
        Map<String, Object> map = new HashMap<>();
        map.put("stuId", 800);
        map.put("name", "stu_7");
        map.put("age", 25);
        map.put("sex", "g");
        map.put("score", "95");
        // mongoDB不能用数组代表多个数据，只能用集合表示
        map.put("like", Arrays.asList("physics", "chemistry", "biology"));
        MongoUtil.insertOne("test", map);
    }

    /* 测试查找一个文档 */
    public void findOne() {
        Bson bson = and(or(eq("age", 23), eq("sex", "g")), eq("stuId", 500));
        Map<String, Object> map = MongoUtil.findOne("test", bson);
        System.out.println(map.toString());
    }

    /* 测试查找符合条件的所有文档  */
    public void findMany() {
        Bson bson = gt("age", 23);
        List<Map<String, Object>> list = MongoUtil.findMany("test", bson);
        for (Map<String, Object> map : list) {
            System.out.println(map.toString());
        }
    }

    /* 测试修改一个文档 */
    public void updateOne() {
        Bson filter = eq("stuId", 700);
//		Document newDoc = new Document("$push", new Document("like","math"));
        Document doc = new Document();
        Document d1 = new Document();
        d1.put("like", "math");
        doc.put("$push", d1);
        MongoUtil.updateOne("test", filter, doc);
        Map<String, Object> map = MongoUtil.findOne("test", eq("stuId", 700));
        System.out.println(map.toString());
    }
}

