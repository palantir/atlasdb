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
import com.palantir.atlasdb.workload.migration.actions.CheckInterfacesAreDisabled;
import com.palantir.atlasdb.workload.migration.actions.EnableClientInterfaces;
import com.palantir.atlasdb.workload.migration.actions.ForceRebuild;
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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class MigratingCassandraCoordinator {
    private static final SafeLogger log = SafeLoggerFactory.get(MigratingCassandraCoordinator.class);
    private final List<MigrationAction> startActions;

    private static final int MAX_MIGRATION_ATTEMPTS = 5;

    private MigratingCassandraCoordinator(List<MigrationAction> startActions) {
        this.startActions = startActions;
    }

    public static MigratingCassandraCoordinator create(
            Supplier<Session> sessionProvider, Collection<String> hosts, MigrationTracker migrationTracker) {
        CqlCassandraKeyspaceReplicationStrategyManager strategyManager =
                new CqlCassandraKeyspaceReplicationStrategyManager(sessionProvider);
        CassandraStateManager allStateManager = CassandraStateManagerFactory.create(hosts);
        CassandraStateManager dc2StateManager = CassandraStateManagerFactory.createDc2StateManager();
        CassandraMetadataManager metadataManager = new DefaultCassandraMetadataManager();
        return create(strategyManager, dc2StateManager, allStateManager, metadataManager, migrationTracker);
    }

    public static MigratingCassandraCoordinator create(
            CqlCassandraKeyspaceReplicationStrategyManager strategyManager,
            CassandraStateManager dc2StateManager,
            CassandraStateManager allNodeStateManager,
            CassandraMetadataManager metadataManager,
            MigrationTracker migrationTracker) {
        Set<String> datacenters = metadataManager.getAllDatacenters().stream()
                .map(Datacenter::datacenter)
                .collect(Collectors.toSet());
        CheckInterfacesAreDisabled checkInterfacesAreDisabledAction = new CheckInterfacesAreDisabled(dc2StateManager);
        AlterKeyspaceDatacenters alterKeyspaceDatacentersAction =
                new AlterKeyspaceDatacenters(strategyManager, allNodeStateManager, datacenters);
        ForceRebuild forceRebuildAction = new ForceRebuild(
                dc2StateManager,
                strategyManager,
                migrationTracker::markRebuildAsStarted,
                metadataManager.sourceDatacenter().datacenter());
        EnableClientInterfaces enableClientInterfaces = new EnableClientInterfaces(dc2StateManager);
        return new MigratingCassandraCoordinator(List.of(
                checkInterfacesAreDisabledAction,
                alterKeyspaceDatacentersAction,
                forceRebuildAction,
                enableClientInterfaces));
    }

    public void runForward() {
        log.info("Starting migration");
        for (int migrationAttempt = 0; migrationAttempt < MAX_MIGRATION_ATTEMPTS; migrationAttempt++) {
            try {
                log.info("Running migration attempt {}", SafeArg.of("migrationAttempt", migrationAttempt));
                startActions.forEach(this::runAction);
                log.info("Migration attempt {} succeeded", SafeArg.of("migrationAttempt", migrationAttempt));
                return;
            } catch (RuntimeException e) {
                log.error("Migration attempt {} failed", SafeArg.of("migrationAttempt", migrationAttempt), e);
            }
        }
        log.error(
                "Failed max migration attempt count {}. Aborting.",
                SafeArg.of("migrationAttempts", MAX_MIGRATION_ATTEMPTS));
        throw new SafeRuntimeException(
                "Failed max migration attempt count. Aborting",
                SafeArg.of("migrationAttempts", MAX_MIGRATION_ATTEMPTS));
    }

    private void runAction(MigrationAction action) {
        String actionName = action.toString();
        if (action.isApplied()) {
            log.info("Action {} already applied", SafeArg.of("action", actionName));
            return;
        }
        log.info("Running action {}", SafeArg.of("action", actionName));
        action.runForwardStep();

        if (!action.isApplied()) {
            throw new SafeRuntimeException("Failed to apply action", SafeArg.of("action", actionName));
        }
    }
}
