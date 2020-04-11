package com.sym.datasource.hikari;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * HikariCP的官方文档：
 * @see <a href="https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby"></a>
 *
 * Created by shenym on 2019/11/22.
 */
public class _HikariCPDataSource {

    /*
     * HikariCP的数据源, 里面定制了一个连接池, 存放JDBC的连接：Connection
     */
    private static HikariDataSource dataSource;

    static {
        // 这种方式是直接设置
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/spring");
        config.setUsername("root");
        config.setPassword("root");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
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
