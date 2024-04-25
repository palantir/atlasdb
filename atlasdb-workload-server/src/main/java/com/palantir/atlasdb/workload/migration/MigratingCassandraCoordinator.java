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

package com.palantir.atlasdb.workload.migration;

import com.datastax.driver.core.Session;
import com.palantir.atlasdb.workload.migration.actions.AlterKeyspaceDatacenters;
import com.palantir.atlasdb.workload.migration.actions.MigrationAction;
import com.palantir.atlasdb.workload.migration.cql.CqlCassandraKeyspaceReplicationStrategyManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraMetadataManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManagerFactory;
import com.palantir.atlasdb.workload.migration.jmx.DefaultCassandraMetadataManager;
import com.palantir.cassandra.manager.core.metadata.Datacenter;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class MigratingCassandraCoordinator {
    private static final SafeLogger log = SafeLoggerFactory.get(MigratingCassandraCoordinator.class);
    private final List<MigrationAction> startActions;

    private MigratingCassandraCoordinator(List<MigrationAction> startActions) {
        this.startActions = startActions;
    }

    public static MigratingCassandraCoordinator create(Supplier<Session> sessionProvider, List<String> hosts) {
        CqlCassandraKeyspaceReplicationStrategyManager strategyManager =
                new CqlCassandraKeyspaceReplicationStrategyManager(sessionProvider);
        CassandraStateManager allStateManager = CassandraStateManagerFactory.create(hosts);
        CassandraMetadataManager metadataManager = new DefaultCassandraMetadataManager();
        return create(strategyManager, allStateManager, metadataManager);
    }

    public static MigratingCassandraCoordinator create(
            CqlCassandraKeyspaceReplicationStrategyManager strategyManager,
            //            CassandraStateManager dc2StateManager,
            CassandraStateManager allNodeStateManager,
            CassandraMetadataManager metadataManager) {
        Set<String> datacenters = metadataManager.getAllDatacenters().stream()
                .map(Datacenter::datacenter)
                .collect(Collectors.toSet());
        AlterKeyspaceDatacenters alterKeyspaceDatacentersAction =
                new AlterKeyspaceDatacenters(strategyManager, allNodeStateManager, datacenters);
        return new MigratingCassandraCoordinator(List.of(alterKeyspaceDatacentersAction));
    }

    public void runForward() {
        startActions.forEach(this::runAction);
    }

    private void runAction(MigrationAction action) {
        String actionName = action.toString();
        if (action.isApplied()) {
            log.info("Action already applied", SafeArg.of("action", actionName));
            return;
        }

        action.runForwardStep();

        if (!action.isApplied()) {
            throw new SafeRuntimeException("Failed to apply action", SafeArg.of("action", actionName));
        }
    }
}
