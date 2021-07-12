package com.sym.datasource.hikari;

import com.sym.util.PropertiesUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * HikariCP的官方文档：
 * https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby
 *
 * @author ym.shen
 * Created on 2019/11/22.
 */
public class SymHikariDatasource {

    /**
     * HikariCP的数据源, 里面定制了一个连接池, 存放JDBC的连接：Connection
     */
    private static HikariDataSource dataSource;

    static {
        // 将 hikari.properties解析成配置文件
        Properties properties = PropertiesUtil.loadProperties("datasource/hikari.properties");
        HikariConfig config = new HikariConfig(properties);
        // 实例化数据源
        dataSource = new HikariDataSource(config);
    }

    /**
     * 获取连接
     */
    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
