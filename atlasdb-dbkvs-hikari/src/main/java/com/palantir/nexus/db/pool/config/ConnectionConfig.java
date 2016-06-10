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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;

import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.nexus.db.DBType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.util.DriverDataSource;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = false)
public abstract class ConnectionConfig {

    public abstract String type();

    public abstract String getHost();
    public abstract int getPort();

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

    @Value.Derived
    public HikariConfig getHikariConfig() {
        // Initialize the Hikari configuration
        HikariConfig config = new HikariConfig();

        Properties props = getProperties();

        config.setPoolName("db-pool-" + getConnId() + "-" + getDbLogin());
        config.setRegisterMbeans(true);
        config.setMetricRegistry(SharedMetricRegistries.getOrCreate("com.palantir.metrics"));

        config.setMinimumIdle(getMinConnections());
        config.setMaximumPoolSize(getMaxConnections());

        config.setMaxLifetime(TimeUnit.SECONDS.toMillis(getMaxConnectionAge()));
        config.setIdleTimeout(TimeUnit.SECONDS.toMillis(getMaxIdleTime()));
        config.setLeakDetectionThreshold(getUnreturnedConnectionTimeout());
        config.setConnectionTimeout(getCheckoutTimeout());

        // TODO: See if driver supports JDBC4 (isValid()) and use it.
        config.setConnectionTestQuery(getTestQuery());

        if (!props.isEmpty()) {
            config.setDataSourceProperties(props);
        }

        config.setJdbcUrl(getUrl());
        config.setDataSource(new DriverDataSource(getUrl(), getDriverClass(), props, null, null));

        return config;
    }

    @Value.Derived
    public abstract Properties getProperties();

}
