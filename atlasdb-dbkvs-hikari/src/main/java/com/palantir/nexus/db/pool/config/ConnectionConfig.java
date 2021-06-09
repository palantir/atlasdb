/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.pool.config;

import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.common.base.Visitors;
import com.palantir.common.visitor.Visitor;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.InterceptorDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.util.DriverDataSource;
import java.sql.Connection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(PostgresConnectionConfig.class),
    @JsonSubTypes.Type(OracleConnectionConfig.class),
    @JsonSubTypes.Type(H2ConnectionConfig.class)
})
public abstract class ConnectionConfig {

    public abstract String type();

    public abstract String getDbLogin();

    /**
     * If a runtime config is present, this does not need to be set and will not be used. A default value is provided
     * so that the config will parse when this field is missing.
     */
    @Value.Default
    public MaskedValue getDbPassword() {
        return ImmutableMaskedValue.of("");
    }

    public abstract String getUrl();

    public abstract String getDriverClass();

    public abstract String getTestQuery();

    @JsonIgnore
    @Value.Derived
    public abstract DBType getDbType();

    @JsonIgnore
    @Value.Derived
    public abstract Optional<String> namespace();

    @Value.Default
    public int getMinConnections() {
        return 8;
    }

    @Value.Default
    public int getMaxConnections() {
        return 256;
    }

    /**
     * STIG O121-C2-016500 and Fedramp requires idle connections are closed within 15 mins.
     * Because typically minimum pool sizes are > 0, we frequently create connections that are
     * never used. Hence, this setting needs to be < 15 min - "a few seconds" (recommended in
     * Hikari docs to allow for client side latency). 900 - a dozen = 888.
     */
    @Value.Default
    public Integer getMaxConnectionAge() {
        return 888;
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
    public String getConnectionPoolIdentifier() {
        return "db-pool";
    }

    @JsonIgnore
    @Value.Derived
    public String getConnectionPoolName() {
        return String.format("%s-%s", getConnectionPoolIdentifier(), getConnId());
    }

    /**
     * This is JsonIgnore'd because it doesn't serialise. Serialisation is needed for atlasdb-dropwizard-bundle.
     */
    @JsonIgnore
    @Value.Default
    public Visitor<Connection> getOnAcquireConnectionVisitor() {
        return Visitors.emptyVisitor();
    }

    @Value.Default
    public Properties getHikariProperties() {
        return new Properties();
    }

    @JsonIgnore
    @Value.Lazy
    public HikariConfig getHikariConfig() {
        // Initialize the Hikari configuration
        HikariConfig config = new HikariConfig();

        Properties props = getHikariProperties();

        config.setPoolName(getConnectionPoolName());
        config.setRegisterMbeans(true);
        config.setMetricRegistry(SharedMetricRegistries.getOrCreate("com.palantir.metrics"));

        config.setMinimumIdle(getMinConnections());
        config.setMaximumPoolSize(getMaxConnections());

        config.setMaxLifetime(TimeUnit.SECONDS.toMillis(getMaxConnectionAge()));
        config.setIdleTimeout(TimeUnit.SECONDS.toMillis(getMaxIdleTime()));
        config.setLeakDetectionThreshold(getUnreturnedConnectionTimeout());

        // Not a bug - we don't want to use connectionTimeout here, since Hikari uses a different terminology.
        // See https://github.com/brettwooldridge/HikariCP/wiki/Configuration
        //   - connectionTimeout = how long to wait for a connection to be opened.
        // ConnectionConfig.connectionTimeoutSeconds is passed in via getHikariProperties(), in subclasses.
        config.setConnectionTimeout(getCheckoutTimeout());

        // TODO (bullman): See if driver supports JDBC4 (isValid()) and use it.
        config.setConnectionTestQuery(getTestQuery());

        if (!props.isEmpty()) {
            config.setDataSourceProperties(props);
        }

        config.setJdbcUrl(getUrl());
        DataSource dataSource = wrapDataSourceWithVisitor(
                new DriverDataSource(getUrl(), getDriverClass(), props, null, null), getOnAcquireConnectionVisitor());
        config.setDataSource(dataSource);

        return config;
    }

    private static DataSource wrapDataSourceWithVisitor(DataSource ds, final Visitor<Connection> visitor) {
        return InterceptorDataSource.wrapInterceptor(new InterceptorDataSource(ds) {
            @Override
            protected void onAcquire(Connection conn) {
                visitor.visit(conn);
            }
        });
    }
}
