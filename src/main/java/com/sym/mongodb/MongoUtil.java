package com.sym.mongodb;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;

public class MongoUtil {

    /**
     * 连接mongodb的客户端
     */
    private static MongoClient client;

    /**
     * 支持多线程，每个用户操作的Mongo的数据库不一样
     */
    private static ThreadLocal<MongoDatabase> threadLocal;

    static {
        // 加载配置文件，获取MongoDB的主机地址和端口号
        Properties prop = new Properties();
        try {
            // 加载配置文件，获取mongo客户端连接
            prop.load(MongoUtil.class.getResourceAsStream("properties/mongo.properties"));
            String host = String.valueOf(prop.get("mongo.host"));
            int port = Integer.parseInt(String.valueOf(prop.get("mongo.port")));
            client = new MongoClient(host, port);
            // 设置本地线程
            threadLocal = new ThreadLocal<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化每个线程对应的数据库
     */
    private static MongoDatabase initDatabase() {
        MongoDatabase database = threadLocal.get();
        if (null != database) {
            // 若值不为空，说明之前已经设置过了
            return database;
        }
		// 说明还未设置过，初始化数据库给线程使用
		database = client.getDatabase("test");
		threadLocal.set(database);
		return database;
    }

    /**
     * 获取当前操作的数据库
     */
    public static MongoDatabase getDatabase() {
        return initDatabase();
    }

    /**
     * 切换当前操作的数据库
     */
    public static MongoDatabase setDatabase(String databaseName) {
        MongoDatabase database = client.getDatabase(databaseName);
        threadLocal.set(database);
        return database;
    }

    /**
     * 获取指定集合
     */
    public static MongoCollection<Document> getCollection(String collection) {
        MongoDatabase database = initDatabase();
        return database.getCollection(collection);
    }

    /**
     * 删除一个集合
     */
    public static boolean deleteCollection(String collection) {
        try {
            MongoDatabase database = initDatabase();
            database.getCollection(collection).drop();
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    /**
     * 插入单个文档数据
     */
    public static void insertOne(String collection, Map<String, Object> param) {
        Document doc = new Document();
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            doc.put(entry.getKey(), entry.getValue());
        }
        insertOne(collection, doc);
    }


    /**
     * 插入单个文档数据
     */
    public static void insertOne(String collection, Document doc) {
        MongoCollection<Document> mongoCollection = getCollection(collection);
        mongoCollection.insertOne(doc);
    }

    /**
     * 插入多个文档数据
     */
    public static void insertMany(String col, List<Document> params) {
        MongoCollection<Document> mc = getCollection(col);
        mc.insertMany(params);
    }

    /**
     * 根据查询条件获取单个文档记录
     */
    public static Map<String, Object> findOne(String collection, Bson bson) {
        MongoCollection<Document> table = getCollection(collection);
        Document document = table.find(bson).first();
        Map<String, Object> map = new HashMap<>();
        assert document != null;
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    /**
     * 根据查询条件获取多个文档记录
     */
    public static List<Map<String, Object>> findMany(String col, Bson bson) {
        List<Map<String, Object>> resList = new ArrayList<>();
        MongoCollection<Document> table = getCollection(col);
        Block<Document> block = new Block<Document>() {
            @Override
            public void apply(Document t) {
                Map<String, Object> map = new HashMap<>();
                for (Map.Entry<String, Object> entry : t.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                resList.add(map);
            }
        };
        table.find(bson).forEach(block);
        return resList;
    }

    /**
     * 返回集合中的所有文档
     */
    public static List<Map<String, Object>> findAll(String collection) {
        MongoCollection<Document> table = getCollection(collection);
        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
        Block<Document> block = t -> {
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<String, Object> e : t.entrySet()) {
                map.put(e.getKey(), e.getValue());
                res.add(map);
            }
        };
        table.find().forEach(block);
        ;
        return res;
    }


    /**
     * 修改一个文档
     */
    public static long updateOne(String col, Bson filter, Document newDoc) {
        MongoCollection<Document> table = getCollection(col);
        UpdateResult res = table.updateOne(filter, newDoc);
        return res.getModifiedCount();
    }


    /**
     * 修改多个文档
     */
    public static long updateMany(String col, Bson filter, Document newDoc) {
        MongoCollection<Document> table = getCollection(col);
        UpdateResult ur = table.updateMany(filter, newDoc);
        return ur.getModifiedCount();
    }

    /**
     * 删除一个文档
     */
    public static long deleteOne(String collection, Bson filter) {
        MongoCollection<Document> table = getCollection(collection);
        DeleteResult dr = table.deleteOne(filter);
        return dr.getDeletedCount();
    }

    /**
     * 删除多个文档
     */
    public static long deleteMany(String collection, Bson filter) {
        MongoCollection<Document> table = getCollection(collection);
        DeleteResult dr = table.deleteMany(filter);
        return dr.getDeletedCount();
    }

    /**
     * 创建索引
     */
    public static boolean createIndex(String collection, Document doc) {
        MongoCollection<Document> table = getCollection(collection);
        try {
            table.createIndex(doc);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
