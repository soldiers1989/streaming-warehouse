package com.tree.finance.bigdata.utils.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.Driver;

import java.sql.Connection;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 14:35
 */
public class ConnectionFactory {

    private ComboPooledDataSource dataSource;

    private ConnectionFactory(ComboPooledDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static class Builder {
        ComboPooledDataSource ds;

        public Builder() {
            this.ds = new ComboPooledDataSource();
        }

        public Builder jdbcUrl(String url) {
            this.ds.setJdbcUrl(url);
            return this;
        }

        public Builder user(String user) {
            this.ds.setUser(user);
            return this;
        }

        public Builder password(String s) {
            this.ds.setPassword(s);
            return this;
        }

        public Builder acquireIncrement(int n) {
            ds.setAcquireIncrement(n);
            return this;
        }

        public Builder initialPoolSize(int n) {
            ds.setInitialPoolSize(n);
            return this;
        }

        public Builder minPollSize(int n) {
            ds.setMinPoolSize(n);
            return this;
        }

        public Builder maxPollSize(int n) {
            ds.setMaxPoolSize(n);
            return this;
        }

        public ConnectionFactory build() {
            try {
                ds.setDriverClass(Driver.class.getName());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return new ConnectionFactory(ds);
        }
    }

    public Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    public void close() {
        this.dataSource.close();
    }
}
