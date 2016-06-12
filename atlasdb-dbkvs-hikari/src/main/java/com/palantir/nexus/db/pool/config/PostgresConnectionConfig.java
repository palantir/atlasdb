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

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.nexus.db.DBType;

@JsonDeserialize(as = ImmutablePostgresConnectionConfig.class)
@JsonSerialize(as = ImmutablePostgresConnectionConfig.class)
@JsonTypeName(PostgresConnectionConfig.TYPE)
@Value.Immutable
public abstract class PostgresConnectionConfig extends ConnectionConfig {

    public static final String TYPE = "postgres";

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
    @Value.Derived
    public Properties getHikariProperties() {
        Properties props = new Properties();

        props.setProperty("user", getDbLogin());
        props.setProperty("password", getDbPassword());

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
