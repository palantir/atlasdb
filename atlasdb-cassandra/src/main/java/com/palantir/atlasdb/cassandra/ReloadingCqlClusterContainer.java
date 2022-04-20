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
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;

/**
 * This container creates new CQL Clusters when the underlying server refreshable changes.
 * After a refresh, previous CQL Clusters are closed to avoid resource leaks.
 *
 * There is no guarantee that a cluster provided via {@link #get} will <i>not</i> be closed whilst in use.
 * Consumers should be resilient to any uses after the cluster has been closed, and where necessary, retry.
 *
 * After {@link #close()} has returned, no further CQL Clusters will be created, and all managed CQL Clusters will be
 * closed.
 */
public final class ReloadingCqlClusterContainer implements Closeable, Supplier<CqlCluster> {
    private static final SafeLogger log = SafeLoggerFactory.get(ReloadingCqlClusterContainer.class);

    private final AtomicReference<Optional<CqlCluster>> currentCqlCluster;
    private final Refreshable<CqlCluster> refreshableCqlCluster;
    private final Disposable refreshableSubscriptionDisposable;

    @GuardedBy("this")
    private boolean isClosed;

    private ReloadingCqlClusterContainer(
            Refreshable<CassandraServersConfig> refreshableCassandraServersConfig,
            CqlClusterFactory cqlClusterFactory) {
        this.isClosed = false;
        this.currentCqlCluster = new AtomicReference<>(Optional.empty());
        this.refreshableCqlCluster = refreshableCassandraServersConfig.map(
                cassandraServersConfig -> createNewCluster(cassandraServersConfig, cqlClusterFactory));

        this.refreshableSubscriptionDisposable = refreshableCqlCluster.subscribe(cqlCluster -> {
            Optional<CqlCluster> maybeClusterToClose = currentCqlCluster.getAndSet(Optional.of(cqlCluster));
            try {
                shutdownCluster(maybeClusterToClose);
            } catch (IOException e) {
                log.warn("Failed to close CQL Cluster. This may result in a resource leak", e);
            }
        });
    }

    public static ReloadingCqlClusterContainer of(
            CassandraClusterConfig cassandraClusterConfig,
            Refreshable<CassandraServersConfig> refreshableCassandraServersConfig,
            Namespace namespace) {
        return of(
                refreshableCassandraServersConfig,
                cassandraServersConfig -> CqlCluster.create(cassandraClusterConfig, cassandraServersConfig, namespace));
    }

    public static ReloadingCqlClusterContainer of(
            Refreshable<CassandraServersConfig> refreshableCassandraServersConfig,
            CqlClusterFactory cqlClusterFactory) {
        return new ReloadingCqlClusterContainer(refreshableCassandraServersConfig, cqlClusterFactory);
    }

    /**
     * Synchronized: See {@link #close()}.
     */
    private synchronized CqlCluster createNewCluster(
            CassandraServersConfig cassandraServersConfig, CqlClusterFactory cqlClusterFactory) {
        if (isClosed) {
            throw new SafeIllegalStateException(
                    "Attempted to create a new cluster after the container was closed. If this happens repeatedly,"
                            + " this is likely a bug in closing the container. Otherwise, it is highly likely that the"
                            + " container was closed at the same time as the server list was updated. If so, this error"
                            + " can be ignored.");
        }
        return cqlClusterFactory.apply(cassandraServersConfig);
    }

    /**
     * Synchronized: A lock is taken out to ensure no new CQL Clusters are created after retrieving the current stored
     * cql cluster to close. By doing so, we avoid closing a cluster and subsequently creating a new one that is
     * never closed.
     */
    @Override
    public synchronized void close() throws IOException {
        isClosed = true;
        refreshableSubscriptionDisposable.dispose();
        Optional<CqlCluster> maybeClusterToClose = currentCqlCluster.get();
        shutdownCluster(maybeClusterToClose);
    }

    /**
     * Gets the latest CqlCluster that reflects any changes in the server list, provided {@link #close()} has not
     * been called.
     *
     * The CQL Cluster returned will be closed after {@link #close} is called, or the server list is refreshed, even
     * if the CQL Cluster is in active use.
     */
    @Override
    public CqlCluster get() {
        return refreshableCqlCluster.get();
    }

    interface CqlClusterFactory extends Function<CassandraServersConfig, CqlCluster> {}

    private void shutdownCluster(Optional<CqlCluster> maybeClusterToClose) throws IOException {
        if (maybeClusterToClose.isPresent()) {
            maybeClusterToClose.get().close();
        }
    }
}
