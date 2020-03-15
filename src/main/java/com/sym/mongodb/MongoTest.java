package com.sym.mongodb;
// 使用静态导入，省去写"Filters."
import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sym.mongodb.domain.Person;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;

/**
 * 测试MongoUtil操作类
 *
 * @author shenym
 */
public class MongoTest {

    /**
     * 测试新增一个文档
     */
    @Test
    public void insertOne() {
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

    /**
     * 测试查找一个文档
     */
    @Test
    public void findOne() {
        Bson bson = and(or(eq("age", 23), eq("sex", "g")), eq("stuId", 500));
        Map<String, Object> map = MongoUtil.findOne("test", bson);
        System.out.println(map.toString());
    }

    /**
     * 测试查找符合条件的所有文档
     */
    @Test
    public void findMany() {
        Bson bson = gt("age", 23);
        List<Map<String, Object>> list = MongoUtil.findMany("test", bson);
        for (Map<String, Object> map : list) {
            System.out.println(map.toString());
        }
    }

    /**
     * 测试修改一个文档
     */
    @Test
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

    /**
     * 这种方式可以让MongoDB一个集合对应Java中的一个JavaBean
     */
    @Test
    public void queryForBean(){
        CodecRegistry registries = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(registries).build();
        com.mongodb.client.MongoClient client = MongoClients.create(settings);
        MongoDatabase database = client.getDatabase("test");
        database.withCodecRegistry(registries);
        MongoCollection<Person> personTable = database.getCollection("person", Person.class);
        personTable = personTable.withCodecRegistry(registries);
        Person person = new Person();
        person.setName("警察");
        person.setDel(false);
        person.setBad(250);
        person.setGood(500);
        personTable.insertOne(person);
    }
}

