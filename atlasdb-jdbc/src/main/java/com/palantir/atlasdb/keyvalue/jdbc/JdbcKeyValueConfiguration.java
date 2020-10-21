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
package com.palantir.atlasdb.keyvalue.jdbc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.jdbc.config.JdbcDataSourceConfiguration;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Optional;
import org.immutables.value.Value;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutableJdbcKeyValueConfiguration.class)
@JsonSerialize(as = ImmutableJdbcKeyValueConfiguration.class)
@JsonTypeName(JdbcKeyValueConfiguration.TYPE)
@Value.Immutable
public abstract class JdbcKeyValueConfiguration implements KeyValueServiceConfig {
    public static final int MAX_TABLE_PREFIX_LENGTH = 6;
    public static final String TYPE = "jdbc";

    @Override
    @JsonIgnore
    @Value.Derived
    public Optional<String> namespace() {
        return Optional.empty();
    }

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

    /**
     * This value should be approximately 32k/4 to avoid https://github.com/pgjdbc/pgjdbc/issues/90. Lowering the value
     * may cause a perf hit and increasing may exceed the parameter limit imposed by the driver.
     **/
    @Value.Default
    public int getBatchSizeForMutations() {
        return 8_000;
    }

    /**
     * This value controls the batching in {@link JdbcKeyValueService#getRows(TableReference, Iterable, ColumnSelection, long)}.
     * Lowering the value may cause a perf hit and increasing may exceed the parameter limit imposed by the driver.
     **/
    @Value.Default
    public int getRowBatchSize() {
        return 1_000;
    }

    public abstract JdbcDataSourceConfiguration getDataSourceConfig();

    @Override
    @Value.Default
    public int concurrentGetRangesThreadPoolSize() {
        return 64;
    }

    @Value.Check
    void check() {
        if (getTablePrefix().length() > MAX_TABLE_PREFIX_LENGTH) {
            throw new SafeIllegalArgumentException(
                    "The table prefix can be at most " + MAX_TABLE_PREFIX_LENGTH + " characters.");
        }
        if (!getTablePrefix().matches("[A-Za-z0-9_]*")) {
            throw new SafeIllegalArgumentException(
                    "The table prefix can only contain letters, numbers, and underscores.");
        }
        if (getBatchSizeForReads() <= 0 || getBatchSizeForReads() > 20_000) {
            throw new SafeIllegalArgumentException(
                    "The batchSizeForReads should be an integer greater than 0 and less than 20,000.");
        }
    }
}
