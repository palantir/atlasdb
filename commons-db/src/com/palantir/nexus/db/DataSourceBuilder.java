package com.palantir.nexus.db;

import java.beans.PropertyVetoException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.manager.DBConfig;
import com.palantir.nexus.db.pool.JdbcConfig;

public class DataSourceBuilder {
    private static final Logger log = LoggerFactory.getLogger(DataSourceBuilder.class);

    public static ComboPooledDataSource createDataSource(DBConfig config) {

        // additional connection properties will go in here
        Properties props = JdbcConfig.getPropertiesFromDbConfig(config);

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setProperties(props);

        // loads the jdbc driver
        try {
            cpds.setDriverClass(config.getType().getDriverName());
        } catch (PropertyVetoException e) {
            throw Throwables.throwUncheckedException(e);
        }

        final String url = config.getUrl();
        cpds.setJdbcUrl(url);

        log.info("JDBC url: " + url); //$NON-NLS-1$
        log.info("JDBC user: " + config.getDbLogin()); //$NON-NLS-1$

        cpds.setAcquireRetryAttempts(config.getNumRetryAttempts());
        if (config.getInitialConnections() > 0) {
            cpds.setInitialPoolSize(config.getInitialConnections() );
        }
        if (config.getInitialConnections() > 0) {
            cpds.setMinPoolSize(config.getMinConnections());
        }
        if (config.getMaxConnections() > 0) {
            cpds.setMaxPoolSize(config.getMaxConnections());
        }
        if (config.getMaxConnectionAge() != null) {
            cpds.setMaxConnectionAge(config.getMaxConnectionAge());
        }
        if (config.getMaxIdleTime() != null) {
            cpds.setMaxIdleTime(config.getMaxIdleTime());
        }
        if (config.getUnreturnedConnectionTimeout() != null) {
            cpds.setUnreturnedConnectionTimeout(config.getUnreturnedConnectionTimeout());
        }
        if (config.getDebugUnreturnedConnectionStackTraces() != null) {
            cpds.setDebugUnreturnedConnectionStackTraces(config.getDebugUnreturnedConnectionStackTraces());
        }
        if (config.getCheckoutTimeout() != null) {
            cpds.setCheckoutTimeout(config.getCheckoutTimeout());
        }

        return cpds;
    }

}
