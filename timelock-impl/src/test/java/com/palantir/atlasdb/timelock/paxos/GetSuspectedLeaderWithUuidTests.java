/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.paxos.LeaderPingerContext;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.junit.After;
import org.junit.Test;
import org.mockito.Answers;

public class GetSuspectedLeaderWithUuidTests {

    private static final UUID LOCAL_UUID = UUID.randomUUID();

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void completesPresentAndMissingSeparately()
            throws InterruptedException, ExecutionException, TimeoutException {
        ClientAwareLeaderPinger remote1 = remoteWithRandomUuid();
        ClientAwareLeaderPinger remote2 = remoteWithRandomUuid();

        GetSuspectedLeaderWithUuid function = functionForRemotes(remote1, remote2);

        TestBatchElement element1 =
                TestBatchElement.of(remote1.underlyingRpcClient().pinger().uuid());
        TestBatchElement element2 = TestBatchElement.random();

        function.accept(ImmutableList.of(element1, element2));

        assertThat(element1.get()).contains(remote1);

        assertThat(element2.get())
                .as("requesting a random uuid returns empty optional")
                .isEmpty();
    }

    @Test
    public void coalescesSingleRequests() throws InterruptedException, ExecutionException, TimeoutException {
        UUID uuid = UUID.randomUUID();
        ClientAwareLeaderPinger remote = remoteWithUuid(uuid);
        GetSuspectedLeaderWithUuid function = functionForRemotes(remote);

        TestBatchElement element = TestBatchElement.of(uuid);
        TestBatchElement elementWithSameUuid = TestBatchElement.of(uuid);

        assertThat(element.result()).isNotEqualTo(elementWithSameUuid.result());

        assertThat(element.argument()).isEqualTo(elementWithSameUuid.argument());

        function.accept(ImmutableList.of(element, elementWithSameUuid));

        assertThat(element.get()).contains(remote);

        assertThat(element.get())
                .as("we're using the same instance for the same requests")
                .isSameAs(elementWithSameUuid.get());
        verify(remote.underlyingRpcClient().pinger(), only()).uuid();
    }

    @Test
    public void cachesPreviouslySeenUuids() throws InterruptedException, ExecutionException, TimeoutException {
        UUID uuid = UUID.randomUUID();
        ClientAwareLeaderPinger remote = remoteWithUuid(uuid);
        GetSuspectedLeaderWithUuid function = functionForRemotes(remote);

        TestBatchElement firstRequest = TestBatchElement.of(uuid);
        function.accept(ImmutableList.of(firstRequest));

        assertThat(firstRequest.get()).contains(remote);
        verify(remote.underlyingRpcClient().pinger(), times(1)).uuid();

        TestBatchElement secondRequest = TestBatchElement.of(uuid);
        function.accept(ImmutableList.of(secondRequest));

        assertThat(secondRequest.get()).contains(remote);
        verifyNoMoreInteractions(remote.underlyingRpcClient().pinger());
    }

    private static ClientAwareLeaderPinger remoteWithUuid(UUID uuid) {
        ClientAwareLeaderPinger mock = mock(ClientAwareLeaderPinger.class, Answers.RETURNS_DEEP_STUBS);
        when(mock.underlyingRpcClient().pinger().uuid()).thenReturn(uuid);
        return mock;
    }

    private static ClientAwareLeaderPinger remoteWithRandomUuid() {
        return remoteWithUuid(UUID.randomUUID());
    }

    private GetSuspectedLeaderWithUuid functionForRemotes(ClientAwareLeaderPinger... remotes) {
        Set<ClientAwareLeaderPinger> clientAwareLeaders = ImmutableSet.copyOf(remotes);

        Set<LeaderPingerContext<BatchPingableLeader>> rpcClients = clientAwareLeaders.stream()
                .map(ClientAwareLeaderPinger::underlyingRpcClient)
                .collect(Collectors.toSet());

        Map<LeaderPingerContext<BatchPingableLeader>, CheckedRejectionExecutorService> executors = Maps.toMap(
                ImmutableList.copyOf(rpcClients), _$ -> new CheckedRejectionExecutorService(executorService));

        return new GetSuspectedLeaderWithUuid(executors, clientAwareLeaders, LOCAL_UUID, Duration.ofSeconds(1));
    }

    @SuppressWarnings("immutables:subtype")
    @Value.Immutable
    interface TestBatchElement extends BatchElement<UUID, Optional<ClientAwareLeaderPinger>> {

        static TestBatchElement random() {
            return of(UUID.randomUUID());
        }

        static TestBatchElement of(UUID uuid) {
            return ImmutableTestBatchElement.of(uuid);
        }

        @Value.Parameter
        @Override
        UUID argument();

        @Value.Lazy
        @Override
        default DisruptorAutobatcher.DisruptorFuture<Optional<ClientAwareLeaderPinger>> result() {
            return new DisruptorAutobatcher.DisruptorFuture<>("test");
        }

        default Optional<ClientAwareLeaderPinger> get()
                throws InterruptedException, ExecutionException, TimeoutException {
            return result().get(1, TimeUnit.SECONDS);
        }
    }
}
