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

package com.palantir.atlasdb.workload.migration.jmx;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.immutables.value.Value;

public interface CassandraStateManager {
    void forceRebuild(String sourceDatacenter, Set<String> keyspaces, Consumer<String> markRebuildStarted);

    Set<String> getRebuiltKeyspaces(String sourceDatacenter);

    Optional<String> getConsensusSchemaVersionFromNode();

    void enableClientInterfaces();

    InterfaceStates getInterfaceState();

    @Value.Immutable
    interface InterfaceStates {
        boolean gossipIsRunning();

        boolean nativeTransportIsRunning();

        boolean rpcServerIsRunning();

        @Value.Derived
        default boolean allInterfacesAreDown() {
            return !gossipIsRunning() && !nativeTransportIsRunning() && !rpcServerIsRunning();
        }

        static Builder builder() {
            return new Builder();
        }

        class Builder extends ImmutableInterfaceStates.Builder {}
    }
}
