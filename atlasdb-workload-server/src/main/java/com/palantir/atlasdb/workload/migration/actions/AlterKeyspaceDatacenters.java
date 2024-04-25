/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration.actions;

import com.datastax.driver.core.KeyspaceMetadata;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.palantir.atlasdb.workload.migration.cql.CassandraKeyspaceReplicationStrategyManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class AlterKeyspaceDatacenters implements MigrationAction {
    private final SafeLogger log = SafeLoggerFactory.get(AlterKeyspaceDatacenters.class);
    private final CassandraKeyspaceReplicationStrategyManager replicationStrategyManager;
    private final CassandraStateManager stateManager;
    private final Set<String> datacenters;

    private final Retryer<Optional<String>> retryer = RetryerBuilder.<Optional<String>>newBuilder()
            .retryIfException()
            .withWaitStrategy(WaitStrategies.exponentialWait())
            .withStopStrategy(StopStrategies.stopAfterAttempt(10))
            .retryIfResult(Optional::isEmpty)
            .build();

    public AlterKeyspaceDatacenters(
            CassandraKeyspaceReplicationStrategyManager replicationStrategyManager,
            CassandraStateManager stateManager,
            Set<String> datacenters) {
        this.replicationStrategyManager = replicationStrategyManager;
        this.stateManager = stateManager;
        this.datacenters = datacenters;
    }

    @Override
    public void runForwardStep() {
        getAllNonSystemKeyspaceNames().forEach(keyspace -> {
            replicationStrategyManager.setReplicationFactorToThreeForDatacenters(datacenters, keyspace);
            waitForConsensusSchemaVersion();
        });
    }

    @Override
    public boolean isApplied() {
        return replicationStrategyManager.isReplicationFactorSetToThreeForDatacentersForAllKeyspaces(datacenters);
    }

    private Stream<String> getAllNonSystemKeyspaceNames() {
        return replicationStrategyManager.getNonSystemKeyspaces().stream().map(KeyspaceMetadata::getName);
    }

    private void waitForConsensusSchemaVersion() {
        try {
            retryer.call(stateManager::getConsensusSchemaVersionFromNode);
        } catch (Exception e) {
            log.info("Failed to get a consensus schema version", e);
        }
    }
}
