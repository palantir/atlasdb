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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class CombinedCassandraStateManager implements CassandraStateManager {
    private final List<CassandraStateManager> stateManagers;

    public CombinedCassandraStateManager(List<CassandraStateManager> stateManagers) {
        this.stateManagers = stateManagers;
    }

    @Override
    public void forceRebuild(String datacenter, String keyspace) {
        stateManagers.forEach(manager -> manager.forceRebuild(datacenter, keyspace));
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
    public void enablingClientInterfaces() {
        stateManagers.forEach(CassandraStateManager::enablingClientInterfaces);
    }
}
