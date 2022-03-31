/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;

abstract class ForwardingCassandraKeyValueServiceRuntimeConfig extends CassandraKeyValueServiceRuntimeConfig {

    protected abstract CassandraKeyValueServiceRuntimeConfig delegate();

    @Override
    public String type() {
        return delegate().type();
    }

    @Override
    public CassandraServersConfig servers() {
        return delegate().servers();
    }

    @Override
    public int replicationFactor() {
        return delegate().replicationFactor();
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return delegate().unresponsiveHostBackoffTimeSeconds();
    }

    @Override
    public int mutationBatchCount() {
        return delegate().mutationBatchCount();
    }

    @Override
    public int mutationBatchSizeBytes() {
        return delegate().mutationBatchSizeBytes();
    }

    @Override
    public int fetchBatchCount() {
        return delegate().fetchBatchCount();
    }

    @Override
    public CassandraCellLoadingConfig cellLoadingConfig() {
        return delegate().cellLoadingConfig();
    }

    @Override
    public Integer sweepReadThreads() {
        return delegate().sweepReadThreads();
    }

    @Override
    public int numberOfRetriesOnSameHost() {
        return delegate().numberOfRetriesOnSameHost();
    }

    @Override
    public int numberOfRetriesOnAllHosts() {
        return delegate().numberOfRetriesOnAllHosts();
    }

    @Override
    public int fetchReadLimitPerRow() {
        return delegate().fetchReadLimitPerRow();
    }

    @Override
    public boolean conservativeRequestExceptionHandler() {
        return delegate().conservativeRequestExceptionHandler();
    }

    @Override
    public CassandraTracingConfig tracing() {
        return delegate().tracing();
    }
}
