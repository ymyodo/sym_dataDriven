package com.sym.datasource.druid;

import com.alibaba.druid.pool.DruidDataSource;
import com.sym.util.PropertiesUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Druid(德鲁伊)的官方文档：https://github.com/alibaba/druid/wiki/%E9%A6%96%E9%A1%B5
 *
 * @author ym.shen
 * @date 2019/11/22
 */
public class SymDruidDataSource {

    /**
     * DruidDataSource 的数据源, 里面定制了一个连接池, 存放JDBC的连接：Connection
     */
    private static DruidDataSource datasource;

    static {
        // 获取配置文件
        Properties properties = PropertiesUtil.loadProperties("datasource/druid.properties");
        datasource = new DruidDataSource();
        datasource.configFromPropety(properties);
    }

    /**
     * 获取连接
     */
    public static Connection getConnection() {
        try {
            return datasource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
