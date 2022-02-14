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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.palantir.nexus.db.DBType;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePostgresConnectionConfig.class)
@JsonSerialize(as = ImmutablePostgresConnectionConfig.class)
@JsonTypeName(PostgresConnectionConfig.TYPE)
@Value.Immutable
public abstract class PostgresConnectionConfig extends ConnectionConfig {

    public static final String TYPE = "postgres";

    public abstract String getHost();

    public abstract int getPort();

    @Override
    @Value.Derived
    @JsonIgnore
    public Optional<String> namespace() {
        return Optional.of(getDbName());
    }

    /**
     * Set arbitrary additional connection parameters.
     * See https://jdbc.postgresql.org/documentation/head/connect.html
     */
    @Value.Default
    public Map<String, String> getConnectionParameters() {
        return ImmutableMap.of();
    }

    @Override
    @Value.Default
    public String getUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s", getHost(), getPort(), getDbName());
    }

    @Override
    @Value.Default
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    @Value.Default
    public String getTestQuery() {
        return "SELECT 1";
    }

    public abstract String getDbName();

    @Override
    @Value.Default
    @Value.Auxiliary
    public Properties getHikariProperties() {
        Properties props = new Properties();

        getConnectionParameters().forEach((key, value) -> {
            if (key.equals("ssl") && value.equals("true")) {
                props.setProperty("sslmode", "require");
            } else {
                props.setProperty(key, value);
            }
        });

        props.setProperty("user", getDbLogin());
        props.setProperty("password", getDbPassword().unmasked());

        props.setProperty("tcpKeepAlive", "true");
        props.setProperty("socketTimeout", Integer.toString(getSocketTimeoutSeconds()));

        props.setProperty("connectTimeout", Integer.toString(getConnectionTimeoutSeconds()));
        props.setProperty("loginTimeout", Integer.toString(getConnectionTimeoutSeconds()));

        return props;
    }

    @Override
    @Value.Derived
    public DBType getDbType() {
        return DBType.POSTGRESQL;
    }

    @Override
    public final String type() {
        return TYPE;
    }
}
