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

import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager.InterfaceStates;
import com.palantir.logsafe.Preconditions;

public class CheckInterfacesAreDisabled implements MigrationAction {
    private final CassandraStateManager dc2StateManager;

    public CheckInterfacesAreDisabled(CassandraStateManager dc2StateManager) {
        this.dc2StateManager = dc2StateManager;
    }

    @Override
    public void runForwardStep() {
        Preconditions.checkState(areInterfacesDisabled(), "Interfaces are not disabled, we've made a mistake!");
    }

    @Override
    public boolean isApplied() {
        return areInterfacesDisabled();
    }

    private boolean areInterfacesDisabled() {
        InterfaceStates interfaceStates = dc2StateManager.getInterfaceState();
        return !interfaceStates.rpcServerIsRunning()
                || !interfaceStates.nativeTransportIsRunning()
                || !interfaceStates.gossipIsRunning();
    }
}
