/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.paxos.Client;

public class DiskNamespaceLoaderTest {
    private static final String NAMESPACE_1 = "namespace_1";
    private static final String NAMESPACE_2 = "namespace_2";

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    public DiskNamespaceLoader diskNamespaceLoader;

    @Before
    public void setup() {
        diskNamespaceLoader = new DiskNamespaceLoader(tempFolder.getRoot().toPath());
        createDirectoryForLeaderForEachClientUseCase(NAMESPACE_1);
        createDirectoryInRootDataDirectory(NAMESPACE_2);
    }

    @Test
    public void doesNotLoadLeaderPaxosAsNamespace() {
        Set<String> namespaces = diskNamespaceLoader.getAllPersistedNamespaces().stream().map(Client::value).collect(
                Collectors.toSet());
        assertThat(namespaces).containsExactlyInAnyOrder(NAMESPACE_1, NAMESPACE_2);
    }

    private void createDirectoryForLeaderForEachClientUseCase(String namespace) {
        if (tempFolder.getRoot().toPath()
                .resolve(PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
                .resolve(PaxosTimeLockConstants.MULTI_LEADER_PAXOS_NAMESPACE)
                .resolve(namespace)
                .toFile()
                .mkdirs()) {
            return;
        }
        throw new RuntimeException("Unexpected error when creating a subdirectory");
    }

    private void createDirectoryInRootDataDirectory(String namespace) {
        if (tempFolder.getRoot().toPath().resolve(namespace).toFile().mkdirs()) {
            return;
        }
        throw new RuntimeException("Unexpected error when creating a subdirectory");
    }
}
