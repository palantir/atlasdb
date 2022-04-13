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
import com.palantir.atlasdb.cassandra.backup.CqlCluster;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory.CassandraClusterConfig;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ReloadingCqlClusterContainer implements Closeable, Supplier<CqlCluster> {
    private static final SafeLogger log = SafeLoggerFactory.get(ReloadingCqlClusterContainer.class);

    private final AtomicReference<Optional<CqlCluster>> lastCqlCluster;
    private final Refreshable<CqlCluster> refreshableCqlCluster;
    private boolean isClosed;

    public ReloadingCqlClusterContainer(
            CassandraClusterConfig cassandraClusterConfig,
            Refreshable<CassandraServersConfig> refreshableCassandraServersConfig,
            Namespace namespace) {
        lastCqlCluster = new AtomicReference<>(Optional.empty());
        isClosed = false;
        refreshableCqlCluster = refreshableCassandraServersConfig.map(
                cassandraServersConfig -> createNewCluster(cassandraClusterConfig, cassandraServersConfig, namespace));

        refreshableCqlCluster.subscribe(cqlCluster -> {
            Optional<CqlCluster> maybeCqlClusterToClose = lastCqlCluster.getAndSet(Optional.of(cqlCluster));
            if (maybeCqlClusterToClose.isPresent()) {
                try {
                    maybeCqlClusterToClose.get().close();
                } catch (IOException e) {
                    log.warn("Failed to close CQL Cluster after reloading server list", e);
                }
            }
        });
    }

    private synchronized CqlCluster createNewCluster(
            CassandraClusterConfig cassandraClusterConfig,
            CassandraServersConfig cassandraServersConfig,
            Namespace namespace) {
        if (isClosed) {
            throw new IllegalStateException();
        }
        return CqlCluster.create(cassandraClusterConfig, cassandraServersConfig, namespace);
    }

    @Override
    public synchronized void close() throws IOException {
        isClosed = true;
        Optional<CqlCluster> maybeCqlClusterToClose = lastCqlCluster.get();
        if (maybeCqlClusterToClose.isPresent()) {
            maybeCqlClusterToClose.get().close();
        }
    }

    @Override
    public CqlCluster get() {
        return refreshableCqlCluster.get();
    }
}
