/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.pool.config;

import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.immutables.value.Value;

import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.common.base.Visitors;
import com.palantir.common.visitor.Visitor;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.InterceptorDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.util.DriverDataSource;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = false)
@JsonSubTypes({@JsonSubTypes.Type(PostgresConnectionConfig.class), @JsonSubTypes.Type(OracleConnectionConfig.class), @JsonSubTypes.Type(H2ConnectionConfig.class)})
public abstract class ConnectionConfig {

    public abstract String type();

    public abstract String getDbLogin();
    public abstract String getDbPassword();

    public abstract String getUrl();
    public abstract String getDriverClass();
    public abstract String getTestQuery();

    @Value.Derived
    public abstract DBType getDbType();

    @Value.Default
    public int getMinConnections() {
        return 8;
    }

    @Value.Default
    public int getMaxConnections() {
        return 256;
    }

    @Value.Default
    public Integer getMaxConnectionAge() {
        return 1800;
    }

    @Value.Default
    public Integer getMaxIdleTime() {
        return 600;
    }

    @Value.Default
    public Integer getUnreturnedConnectionTimeout() {
        return 0;
    }

    @Value.Default
    public Integer getCheckoutTimeout() {
        return 30000;
    }

    @Value.Default
    public String getConnId() {
        return "atlas";
    }

    @Value.Default
    public int getSocketTimeoutSeconds() {
        return 120;
    }

    @Value.Default
    public int getConnectionTimeoutSeconds() {
        return 45;
    }

    @Value.Default
    public Visitor<Connection> getOnAcquireConnectionVisitor() {
        return Visitors.emptyVisitor();
    }

    @Value.Default
    public Properties getHikariProperties() {
        return new Properties();
    }

    public static HikariConfig getHikariConfigFromConnectionConfig(ConnectionConfig config) {
        // Initialize the Hikari configuration
        HikariConfig hikariConfig = new HikariConfig();

        Properties props = config.getHikariProperties();

        hikariConfig.setPoolName("db-pool-" + config.getConnId() + "-" + config.getDbLogin());
        hikariConfig.setRegisterMbeans(true);
        hikariConfig.setMetricRegistry(SharedMetricRegistries.getOrCreate("com.palantir.metrics"));

        hikariConfig.setMinimumIdle(config.getMinConnections());
        hikariConfig.setMaximumPoolSize(config.getMaxConnections());

        hikariConfig.setMaxLifetime(TimeUnit.SECONDS.toMillis(config.getMaxConnectionAge()));
        hikariConfig.setIdleTimeout(TimeUnit.SECONDS.toMillis(config.getMaxIdleTime()));
        hikariConfig.setLeakDetectionThreshold(config.getUnreturnedConnectionTimeout());
        hikariConfig.setConnectionTimeout(config.getCheckoutTimeout());

        // TODO: See if driver supports JDBC4 (isValid()) and use it.
        hikariConfig.setConnectionTestQuery(config.getTestQuery());

        if (!props.isEmpty()) {
            hikariConfig.setDataSourceProperties(props);
        }

        hikariConfig.setJdbcUrl(config.getUrl());
        DataSource dataSource = wrapDataSourceWithVisitor(
                new DriverDataSource(config.getUrl(), config.getDriverClass(), props, null, null),
                config.getOnAcquireConnectionVisitor());
        hikariConfig.setDataSource(dataSource);

        return hikariConfig;
    }

    private static DataSource wrapDataSourceWithVisitor(DataSource ds, final Visitor<Connection> visitor) {
        return InterceptorDataSource.wrapInterceptor(new InterceptorDataSource(ds) {
            @Override
            protected void onAcquire(Connection c) {
                visitor.visit(c);
            }
        });
    }

}
