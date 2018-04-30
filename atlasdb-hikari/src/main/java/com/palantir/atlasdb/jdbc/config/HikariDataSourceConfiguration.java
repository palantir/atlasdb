/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.jdbc.config;

import java.util.Properties;

import javax.sql.DataSource;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@AutoService(JdbcDataSourceConfiguration.class)
@Value.Immutable
@JsonTypeName(HikariDataSourceConfiguration.TYPE)
@JsonSerialize(as = ImmutableHikariDataSourceConfiguration.class)
@JsonDeserialize(as = ImmutableHikariDataSourceConfiguration.class)
public abstract class HikariDataSourceConfiguration implements JdbcDataSourceConfiguration {
    public static final String TYPE = "hikari";

    @Override
    public String getType() {
        return TYPE;
    }

    public abstract Properties getProperties();

    @Override
    public DataSource createDataSource() {
        HikariConfig hikariConfig = new HikariConfig(getProperties());
        return new HikariDataSource(hikariConfig);
    }
}
