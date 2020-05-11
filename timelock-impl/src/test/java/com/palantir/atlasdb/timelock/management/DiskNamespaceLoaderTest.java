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

import java.io.File;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

public class DiskNamespaceLoaderTest {
    private static final String NAMESPACE_1 = "namespace_1";
    private static final String NAMESPACE_2 = "namespace_2";


    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private DiskNamespaceLoader diskNamespaceLoader;

    @Before
    public void setup() {
        createDirectoryForLeaderForEachClientUseCase(NAMESPACE_1);
        createDirectoryTimestampUseCase(NAMESPACE_2);
        createDirectoryTimestampUseCase(PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE);
        diskNamespaceLoader = new DiskNamespaceLoader(tempFolder.getRoot().toPath());
    }

    @Test
    public void doesNotLoadLeaderPaxosAsNamespace() {
        Set<String> namespaces = diskNamespaceLoader.getAllPersistedNamespaces().stream().map(client -> client.value()).collect(
                Collectors.toSet());
        assertThat(namespaces).containsExactlyInAnyOrder(NAMESPACE_1, NAMESPACE_2);
    }

    private void createDirectoryForLeaderForEachClientUseCase(String namespace) {
        new File(tempFolder.getRoot().toPath() + "/" +
                Paths.get(PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.MULTI_LEADER_PAXOS_NAMESPACE).toString() + "/" +
                namespace).mkdirs();
    }

    private void createDirectoryTimestampUseCase(String namespace) {
        new File(tempFolder.getRoot().toPath() + "/" +
                namespace).mkdirs();
    }

    @After
    public void cleanup() {
        tempFolder.delete();
    }
}
