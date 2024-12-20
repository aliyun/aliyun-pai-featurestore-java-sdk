package com.aliyun.openservices.paifeaturestore.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;
/*  This class contains some Settings for the Hologres Configuration DataSource.*/
public class Hologres {
    DataSource dataSource;

    public Hologres(String dsn) throws Exception {
        Properties properties = new Properties();

        properties.setProperty("url", dsn);
        properties.setProperty("initialSize", "2");
        properties.setProperty("minIdle", "5");
        properties.setProperty("maxActive", "30");
        properties.setProperty("maxWait", "60000");
        properties.setProperty("timeBetweenEvictionRunsMillis", "2000");
        properties.setProperty("minEvictableIdleTimeMillis", "600000");
        properties.setProperty("maxEvictableIdleTimeMillis", "3600000");
        properties.setProperty("validationQuery", "select 1");
        properties.setProperty("testWhileIdle", "true");
        properties.setProperty("testOnBorrow", "false");
        properties.setProperty("testOnReturn", "false");
        properties.setProperty("keepAlive", "true");
        properties.setProperty("phyMaxUseCount", "100000");
        properties.setProperty("filters", "stat");
        properties.setProperty("poolPreparedStatements", "true");
        properties.setProperty("maxPoolPreparedStatementPerConnectionSize", "10");
        //Create a data source based on the configuration
        DataSource dataSource = DruidDataSourceFactory.createDataSource(properties);

        // first init connection
        try (Connection connection = dataSource.getConnection()) {//获取连接
        } catch (Exception e) {
            throw  e;
        }

        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void close() throws Exception {
        if (null != this.dataSource && this.dataSource instanceof DruidDataSource) {
            ((DruidDataSource) this.dataSource).close();
        }
    }
}
