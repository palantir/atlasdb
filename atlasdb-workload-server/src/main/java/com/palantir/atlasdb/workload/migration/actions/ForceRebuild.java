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
import com.palantir.atlasdb.workload.migration.cql.CassandraKeyspaceReplicationStrategyManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import java.util.stream.Collectors;

public class ForceRebuild implements MigrationAction {
    private final CassandraStateManager dc2StateManager;
    private final CassandraKeyspaceReplicationStrategyManager replicationStrategyManager;
    private final String sourceDatacenter;

    public ForceRebuild(
            CassandraStateManager dc2StateManager,
            CassandraKeyspaceReplicationStrategyManager replicationStrategyManager,
            String sourceDatacenter) {
        this.dc2StateManager = dc2StateManager;
        this.replicationStrategyManager = replicationStrategyManager;
        this.sourceDatacenter = sourceDatacenter;
    }

    @Override
    public void runForwardStep() {
        dc2StateManager.forceRebuild(
                sourceDatacenter,
                replicationStrategyManager.getNonSystemKeyspaces().stream()
                        .map(KeyspaceMetadata::getName)
                        .collect(Collectors.toSet()));
    }

    @Override
    public boolean isApplied() {
        return false;
    }
}
