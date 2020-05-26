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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.paxos.SqliteConnections;
import com.palantir.tokens.auth.AuthHeader;

public class DiskNamespaceLoaderTest {
    private static final String NAMESPACE_1 = "namespace_1";
    private static final String NAMESPACE_2 = "namespace_2";
    public static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private static final PersistentNamespaceContext persistentNamespaceContext = mock(PersistentNamespaceContext.class);
    private static final TimelockNamespaces timelockNamespaces = mock(TimelockNamespaces.class);
    private static final RedirectRetryTargeter redirectRetryTargeter = mock(RedirectRetryTargeter.class);
    private TimeLockManagementResource timeLockManagementResource;

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() {
        Path rootFolderPath = tempFolder.getRoot().toPath();
        when(persistentNamespaceContext.fileDataDirectory()).thenReturn(rootFolderPath);
        when(persistentNamespaceContext.sqliteConnectionSupplier()).thenReturn(SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(rootFolderPath));
        timeLockManagementResource = TimeLockManagementResource.create(persistentNamespaceContext,
                timelockNamespaces,
                redirectRetryTargeter);
        createDirectoryForLeaderForEachClientUseCase(NAMESPACE_1);
        createDirectoryInRootDataDirectory(NAMESPACE_2);
    }

    @Test
    public void doesNotLoadLeaderPaxosAsNamespace() throws ExecutionException, InterruptedException {
        Set<String> namespaces = timeLockManagementResource.getNamespaces(AUTH_HEADER).get();
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
