package com.sym.util;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * 解析 properties 文件, 将其映射成对应实体类
 *
 * @author ym.shen
 * Created on 2020/4/11 14:50
 */
public class PropertiesUtil {

    private final static String SUPPORT_SUFFIX = ".properties";
    private final static String STRING_VALUE_NULL = "null";

    /**
     * 指定配置文件的相对路径, 将其加载成 properties 文件
     *
     * @param path 文件路径
     * @return properties文件
     */
    public static Properties loadProperties(String path) {
        // 获取类加载器, 获取其 url 对象
        ClassLoader classLoader = PropertiesUtil.class.getClassLoader();
        URL url = classLoader.getResource(path);
        if (null == url) {
            throw new IllegalArgumentException("path[" + path + "] not found");
        }

        InputStream inputStream = null;
        try {
            // 与其建立连接, 获取输入流
            URLConnection urlConnection = url.openConnection();
            inputStream = urlConnection.getInputStream();

            // 加载为 properties 文件返回
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;

        } catch (IOException e) {
            throw new LoadFailureException("load fail");
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static <T> T parseProperties(Class<T> t, String path) {
        if (null == t || StringUtils.isBlank(path)) {
            throw new NullPointerException("parameter not be null");
        }
        if (!path.endsWith(SUPPORT_SUFFIX)) {
            throw new UnsupportedOperationException("only support .properties");
        }
        // 获取 properties 对象
        Properties properties = loadProperties(path);

        // 创建实例对象
        T instance;
        try {
            instance = t.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new InstanceFailException(e);
        }

        // 依次解析
        try {
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String key = String.valueOf(entry.getKey());
                String value = String.valueOf(entry.getValue());
                Field field = t.getDeclaredField(key);
                field.setAccessible(true);
                initField(instance, field, value);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new FieldParseFailException(e);
        } catch (ClassNotFoundException | InstantiationException e) {
            throw new InstanceFailException(e);
        } catch (Exception e) {
            throw new ParseFailureException(e);
        }
        return instance;
    }

    /**
     * 设置属性值
     *
     * @param instance      对象实例
     * @param field         对象字段
     * @param propertyValue 未解析的字段值
     */
    private static void initField(Object instance, Field field, String propertyValue) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        field.setAccessible(true);
        // properties文件未指定值, 直接返回
        if (StringUtils.isBlank(propertyValue)) {
            return;
        }
        // 设置值为null
        if (STRING_VALUE_NULL.equalsIgnoreCase(propertyValue)) {
            field.set(instance, null);
        }
        // 获取字段的类型
        setSimpleValue(instance, field, propertyValue);
    }

    /**
     * 设置简单值
     *
     * @param instance      对象实例
     * @param field         对象字段
     * @param propertyValue 待配置的值
     * @throws IllegalAccessException
     */
    private static void setSimpleValue(Object instance, Field field, String propertyValue) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        Class<?> fieldType = field.getType();
        if (fieldType == int.class) {
            field.setInt(instance, Integer.parseInt(propertyValue));
        } else if (fieldType == double.class) {
            field.setDouble(instance, Double.parseDouble(propertyValue));
        } else if (fieldType == long.class) {
            field.setLong(instance, Long.parseLong(propertyValue));
        } else if (fieldType == short.class) {
            field.setShort(instance, Short.parseShort(propertyValue));
        } else if (fieldType == float.class) {
            field.setFloat(instance, Float.parseFloat(propertyValue));
        } else if (fieldType == char.class) {
            field.setChar(instance, propertyValue.charAt(0));
        } else if (fieldType == boolean.class) {
            field.setBoolean(instance, Boolean.parseBoolean(propertyValue));
        } else if (fieldType == String.class) {
            field.set(instance, propertyValue);
        } else if (Collection.class.isAssignableFrom(fieldType)) {
            //TODO 集合的赋值
        } else if (Map.class.isAssignableFrom(fieldType)) {
            //TODO Map的赋值
        } else if (fieldType.isArray()) {
            //TODO 数组的赋值
        } else {
            // 对象的赋值
            Class<?> aClass = Class.forName(propertyValue);
            field.set(instance, aClass.newInstance());
        }

    }


    /**
     * 加载异常
     */
    static class LoadFailureException extends RuntimeException {
        public LoadFailureException() {
            super();
        }

        public LoadFailureException(String message) {
            super(message);
        }

        public LoadFailureException(Throwable t) {
            super(t);
        }
    }

    /**
     * 解析异常
     */
    static class ParseFailureException extends RuntimeException {
        public ParseFailureException() {
            super();
        }

        public ParseFailureException(String message) {
            super(message);
        }

        public ParseFailureException(Throwable t) {
            super(t);
        }
    }

    /**
     * 实例化失败
     */
    static class InstanceFailException extends RuntimeException {
        public InstanceFailException() {
            super();
        }

        public InstanceFailException(String message) {
            super(message);
        }

        public InstanceFailException(Throwable t) {
            super(t);
        }
    }

    /**
     * 解析变量失败
     */
    static class FieldParseFailException extends RuntimeException {
        public FieldParseFailException() {
            super();
        }

        public FieldParseFailException(String message) {
            super(message);
        }

        public FieldParseFailException(Throwable t) {
            super(t);
        }
    }
}
