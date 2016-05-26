package com.palantir.nexus.db.pool;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.palantir.nexus.db.manager.DBConfig;

public class JdbcConfig {

    public static Properties getPropertiesFromDbConfig(DBConfig dbConfig) {
        Properties properties = new Properties();

        properties.setProperty("user", dbConfig.getDbLogin());
        if (dbConfig.getDbDecryptedPassword() != null) {
            properties.setProperty("password", dbConfig.getDbDecryptedPassword());
        }

        Integer socketTimeout = dbConfig.getSocketTimeoutSeconds();
        Integer connectTimeout = dbConfig.getConnectTimeoutSeconds();

        if (socketTimeout != null) {
            /* Oracle */
            properties.setProperty("oracle.net.keepAlive", "true");
            properties.setProperty("oracle.jdbc.ReadTimeout", Long.toString(TimeUnit.SECONDS.toMillis(socketTimeout)));

            /* Postgres */
            properties.setProperty("tcpKeepAlive", "true");
            properties.setProperty("socketTimeout", socketTimeout.toString());
        }
        if (connectTimeout != null) {
            /* Oracle */
            properties.setProperty("oracle.net.CONNECT_TIMEOUT", Long.toString(TimeUnit.SECONDS.toMillis(connectTimeout)));

            /* Postgres */
            properties.setProperty("connectTimeout", connectTimeout.toString());
            properties.setProperty("loginTimeout", connectTimeout.toString());
        }

        properties.setProperty("oracle.jdbc.maxCachedBufferSize", "100000");
        return properties;
    }

}
