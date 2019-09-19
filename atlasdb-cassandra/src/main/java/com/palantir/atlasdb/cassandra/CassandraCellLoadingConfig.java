/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import org.immutables.value.Value;

/**
 * Specifies limits on query sizes when loading cells from an underlying Cassandra key-value service.
 *
 * Specifying large limits may allow for greater overall throughput across nodes as there are overall fewer RPCs,
 * but may increase the latency of individual queries owing to implementation reasons.
 *
 * Limits provided should be positive; also, the cross column load batch limit should not exceed the single query
 * load batch limit.
 */
@JsonSerialize(as = ImmutableCassandraCellLoadingConfig.class)
@JsonDeserialize(as = ImmutableCassandraCellLoadingConfig.class)
@Value.Immutable
public abstract class CassandraCellLoadingConfig {
    /**
     * Implementations of Cassandra KVS may combine requests for different columns in a single call to the database.
     * These "merged" calls will have size no larger than this value.
     *
     * To disable cross-column batching, this value may be set to 1.
     */
    @Value.Default
    public int crossColumnLoadBatchLimit() {
        return CassandraConstants.DEFAULT_CROSS_COLUMN_LOAD_BATCH_LIMIT;
    }

    /**
     * Implementations should not make requests for more than this many cells in a single call to the database.
     */
    @Value.Default
    public int singleQueryLoadBatchLimit() {
        return CassandraConstants.DEFAULT_SINGLE_QUERY_LOAD_BATCH_LIMIT;
    }

    @Value.Check
    public void check() {
        Preconditions.checkState(
                crossColumnLoadBatchLimit() > 0,
                "crossColumnLoadBatchLimit should be positive, but found %s",
                crossColumnLoadBatchLimit());
        Preconditions.checkState(
                singleQueryLoadBatchLimit() > 0,
                "singleQueryLoadBatchLimit should be positive, but found %s",
                singleQueryLoadBatchLimit());
        Preconditions.checkState(crossColumnLoadBatchLimit() <= singleQueryLoadBatchLimit(),
                "Cross column load batch limit %s shouldn't exceed single query load batch limit %s",
                crossColumnLoadBatchLimit(),
                singleQueryLoadBatchLimit());
    }

    static CassandraCellLoadingConfig defaultConfig() {
        return ImmutableCassandraCellLoadingConfig.builder().build();
    }

    static CassandraCellLoadingConfig of(int crossColumnLoadBatchLimit, int singleQueryLoadBatchLimit) {
        return ImmutableCassandraCellLoadingConfig.builder()
                .crossColumnLoadBatchLimit(crossColumnLoadBatchLimit)
                .singleQueryLoadBatchLimit(singleQueryLoadBatchLimit)
                .build();
    }
}
