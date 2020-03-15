package com.sym.datasource.druid;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Druid(德鲁伊)的官方文档：
 * @see <a href="https://github.com/alibaba/druid/wiki/%E9%A6%96%E9%A1%B5"></a>
 *
 * Created by shenym on 2019/11/22.
 */
public class _DruidDataSource {

    /*
     * DruidDataSource 的数据源, 里面定制了一个连接池, 存放JDBC的连接：Connection
     */
    private static DruidDataSource dataSource;

    static {
        dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:mysql://localhost:3306/spring");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
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
