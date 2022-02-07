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

import static com.palantir.conjure.java.api.testing.Assertions.assertThatServiceExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.AuthHeaderValidator;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.paxos.SqliteConnections;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tokens.auth.BearerToken;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimeLockManagementResourceTest {
    private static final String NAMESPACE_1 = "namespace_1";
    private static final String NAMESPACE_2 = "namespace_2";
    private static final Set<Namespace> NAMESPACES =
            ImmutableSet.of(Namespace.of(NAMESPACE_1), Namespace.of(NAMESPACE_2));
    private static final BearerToken BEARER_TOKEN = BearerToken.valueOf("bear");
    private static final BearerToken WRONG_BEARER_TOKEN = BearerToken.valueOf("tiger");
    private static final AuthHeader AUTH_HEADER = AuthHeader.of(BEARER_TOKEN);
    private static final AuthHeader WRONG_AUTH_HEADER = AuthHeader.of(WRONG_BEARER_TOKEN);

    @Mock
    private AuthHeaderValidator authHeaderValidator;

    @Mock
    private AllNodesDisabledNamespacesUpdater allNodesDisabledNamespacesUpdater;

    @Mock
    private Function<String, TimeLockServices> serviceFactory;

    @Mock
    private Supplier<Integer> maxNumberOfClientsSupplier;

    @Mock
    private Runnable serviceStopper;

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private TimeLockManagementResource timeLockManagementResource;

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws MalformedURLException {
        URL testUrl = new URL("http", "host", "file");
        RedirectRetryTargeter redirectRetryTargeter = RedirectRetryTargeter.create(testUrl, ImmutableList.of(testUrl));

        Path rootFolderPath = tempFolder.getRoot().toPath();
        PersistentNamespaceContext persistentNamespaceContext = PersistentNamespaceContexts.timestampBoundPaxos(
                rootFolderPath, SqliteConnections.getDefaultConfiguredPooledDataSource(rootFolderPath));

        DisabledNamespaces disabledNamespaces = mock(DisabledNamespaces.class);
        TimelockNamespaces namespaces =
                new TimelockNamespaces(metricsManager, serviceFactory, maxNumberOfClientsSupplier, disabledNamespaces);

        when(authHeaderValidator.suppliedTokenIsValid(AUTH_HEADER)).thenReturn(true);
        when(authHeaderValidator.suppliedTokenIsValid(WRONG_AUTH_HEADER)).thenReturn(false);
        timeLockManagementResource = TimeLockManagementResource.create(
                persistentNamespaceContext,
                namespaces,
                allNodesDisabledNamespacesUpdater,
                authHeaderValidator,
                redirectRetryTargeter,
                new ServiceLifecycleController(serviceStopper, PTExecutors.newSingleThreadScheduledExecutor()));

        createDirectoryForLeaderForEachClientUseCase(NAMESPACE_1);
        createDirectoryInRootDataDirectory(NAMESPACE_2);
    }

    @Test
    public void disableTimeLockThrowsIfAuthHeaderIsWrong() {
        assertThatServiceExceptionThrownBy(() -> AtlasFutures.getUnchecked(
                        timeLockManagementResource.disableTimelock(WRONG_AUTH_HEADER, NAMESPACES)))
                .hasType(ErrorType.PERMISSION_DENIED);
        verifyNoInteractions(allNodesDisabledNamespacesUpdater);
    }

    @Test
    public void disableTimeLockCallsUpdater() {
        timeLockManagementResource.disableTimelock(AUTH_HEADER, NAMESPACES);
        verify(allNodesDisabledNamespacesUpdater).disableOnAllNodes(NAMESPACES);
    }

    @Test
    public void reEnableTimeLockThrowsIfAuthHeaderIsWrong() {
        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(NAMESPACES, UUID.randomUUID());
        assertThatServiceExceptionThrownBy(() -> AtlasFutures.getUnchecked(
                        timeLockManagementResource.reenableTimelock(WRONG_AUTH_HEADER, request)))
                .hasType(ErrorType.PERMISSION_DENIED);
        verifyNoInteractions(allNodesDisabledNamespacesUpdater);
    }

    @Test
    public void reEnableTimeLockCallsUpdater() {
        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(NAMESPACES, UUID.randomUUID());
        timeLockManagementResource.reenableTimelock(AUTH_HEADER, request);
        verify(allNodesDisabledNamespacesUpdater).reEnableOnAllNodes(request);
    }

    @Test
    public void doesNotLoadLeaderPaxosAsNamespace() throws ExecutionException, InterruptedException {
        Set<String> namespaces =
                timeLockManagementResource.getNamespaces(AUTH_HEADER).get();
        assertThat(namespaces).containsExactlyInAnyOrder(NAMESPACE_1, NAMESPACE_2);
    }

    private void createDirectoryForLeaderForEachClientUseCase(String namespace) {
        if (tempFolder
                .getRoot()
                .toPath()
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
