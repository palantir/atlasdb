/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;

@AutoService(KeyValueServiceRuntimeConfig.class)
@JsonDeserialize(as = ImmutableCassandraKeyValueServiceRuntimeConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceRuntimeConfig.class)
@Value.Immutable
public class CassandraKeyValueServiceRuntimeConfig implements KeyValueServiceRuntimeConfig {
    @Override
    public String type() {
        return "cassandra";
    }

    /**
     * The minimal period we wait to check if a Cassandra node is healthy after it's been blacklisted.
     */
    @Value.Default
    public int unresponsiveHostBackoffTimeSeconds() {
        return 30;
    }

    public int mutationBatchCount() {
        return 10;
    }

    public int mutationBatchSizeBytes() {
        return 10;
    }

    public int fetchBatchCount() {
        return 10;
    }

    public Integer sweepReadThreads() {
        return 10;
    }

    public int numberOfRetriesOnSameHost() {
        return 3;
    }

    public int numberOfRetriesOnAllHosts() {
        return 6;
    }
}
