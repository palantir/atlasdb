/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.jdbc;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.jdbc.config.JdbcDataSourceConfiguration;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutableJdbcKeyValueConfiguration.class)
@JsonSerialize(as = ImmutableJdbcKeyValueConfiguration.class)
@JsonTypeName(JdbcKeyValueConfiguration.TYPE)
@Value.Immutable
public abstract class JdbcKeyValueConfiguration implements KeyValueServiceConfig {
    public static final int MAX_TABLE_PREFIX_LENGTH = 6;
    public static final String TYPE = "jdbc";

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Default
    public String getTablePrefix() {
        return "at_";
    }

    /**
     * This value should be approximately 32k/3 to avoid https://github.com/pgjdbc/pgjdbc/issues/90. Lowering the value
     * may cause a perf hit and increasing may exceed the parameter limit imposed by the driver.
    **/
    @Value.Default
    public int getBatchSizeForReads() {
        return 10_000;
    }

    public abstract JdbcDataSourceConfiguration getDataSourceConfig();

    @Value.Check
    void check() {
        if (getTablePrefix().length() > MAX_TABLE_PREFIX_LENGTH) {
            throw new IllegalArgumentException("The table prefix can be at most " + MAX_TABLE_PREFIX_LENGTH + " characters.");
        }
        if (!getTablePrefix().matches("[A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("The table prefix can only contain letters, numbers, and underscores.");
        }
        if (getBatchSizeForReads() <= 0 || getBatchSizeForReads() > 20_000) {
            throw new IllegalArgumentException("The batchSizeForReads should be an integer greater than 0 and less than 20,000.");
        }
    }
}
