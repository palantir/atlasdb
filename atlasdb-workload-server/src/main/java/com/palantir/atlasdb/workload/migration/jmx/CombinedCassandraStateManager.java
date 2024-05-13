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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import one.util.streamex.StreamEx;

public final class CombinedCassandraStateManager implements CassandraStateManager {
    private final List<CassandraStateManager> stateManagers;

    public CombinedCassandraStateManager(List<CassandraStateManager> stateManagers) {
        this.stateManagers = stateManagers;
    }

    @Override
    public Set<Callable<Boolean>> forceRebuildCallables(
            String sourceDatacenter, Set<String> keyspaces, Consumer<String> markRebuildAsStarted) {
        return StreamEx.of(stateManagers)
                .map(stateManager ->
                        stateManager.forceRebuildCallables(sourceDatacenter, keyspaces, markRebuildAsStarted))
                .flatMap(StreamEx::of)
                .toImmutableSet();
    }

    @Override
    public boolean isRebuilding() {
        return StreamEx.of(stateManagers).anyMatch(CassandraStateManager::isRebuilding);
    }

    @Override
    public Set<String> getRebuiltKeyspaces(String sourceDatacenter) {
        return stateManagers.stream()
                .map(stateManager -> stateManager.getRebuiltKeyspaces(sourceDatacenter))
                .reduce(Sets::intersection)
                .orElseGet(Set::of);
    }

    @Override
    public Optional<String> getConsensusSchemaVersionFromNode() {
        List<Optional<String>> schemaVersions = stateManagers.stream()
                .map(CassandraStateManager::getConsensusSchemaVersionFromNode)
                .collect(Collectors.toList());
        Optional<String> firstElement = schemaVersions.get(0); // It's fine if it throws in this hacky version
        if (firstElement.isPresent() && schemaVersions.stream().allMatch(firstElement::equals)) {
            return firstElement;
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void enableClientInterfaces() {
        stateManagers.forEach(CassandraStateManager::enableClientInterfaces);
    }

    @Override
    public InterfaceStates getInterfaceState() {
        List<InterfaceStates> interfaceStates = stateManagers.stream()
                .map(CassandraStateManager::getInterfaceState)
                .collect(Collectors.toList());
        return InterfaceStates.builder()
                .gossipIsRunning(interfaceStates.stream().allMatch(InterfaceStates::gossipIsRunning))
                .nativeTransportIsRunning(interfaceStates.stream().allMatch(InterfaceStates::nativeTransportIsRunning))
                .rpcServerIsRunning(interfaceStates.stream().allMatch(InterfaceStates::rpcServerIsRunning))
                .build();
    }

    @Override
    public void setInterDcStreamThroughput(double throughput) {
        stateManagers.forEach(stateManager -> stateManager.setInterDcStreamThroughput(throughput));
    }

    @Override
    public double getInterDcStreamThroughput() {
        Set<Double> interDcStreamThroughputs = stateManagers.stream()
                .map(CassandraStateManager::getInterDcStreamThroughput)
                .collect(Collectors.toSet());
        if (interDcStreamThroughputs.size() > 1) {
            throw new SafeRuntimeException("DC stream throughput is not consistent across all nodes");
        }
        return Iterables.getOnlyElement(interDcStreamThroughputs);
    }
}
