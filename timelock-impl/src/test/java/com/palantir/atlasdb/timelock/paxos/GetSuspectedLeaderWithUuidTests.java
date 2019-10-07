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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.immutables.value.Value;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;

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
        ClientAwarePingableLeader remote1 = remoteWithRandomUuid();
        ClientAwarePingableLeader remote2 = remoteWithRandomUuid();

        GetSuspectedLeaderWithUuid function = functionForRemotes(remote1, remote2);

        TestBatchElement element1 = TestBatchElement.of(remote1.uuid());
        TestBatchElement element2 = TestBatchElement.random();

        function.accept(ImmutableList.of(element1, element2));

        assertThat(element1.get())
                .contains(remote1);

        assertThat(element2.get())
                .as("requesting a random uuid returns empty optional")
                .isEmpty();
    }

    @Test
    public void coalescesSingleRequests() throws InterruptedException, ExecutionException, TimeoutException {
        UUID uuid = UUID.randomUUID();
        ClientAwarePingableLeader remote = remoteWithUuid(uuid);
        GetSuspectedLeaderWithUuid function = functionForRemotes(remote);

        TestBatchElement element = TestBatchElement.of(uuid);
        TestBatchElement elementWithSameUuid = TestBatchElement.of(uuid);

        assertThat(element.result())
                .isNotEqualTo(elementWithSameUuid.result());

        assertThat(element.argument())
                .isEqualTo(elementWithSameUuid.argument());

        function.accept(ImmutableList.of(element, elementWithSameUuid));

        assertThat(element.get()).contains(remote);

        assertThat(element.get())
                .as("we're using the same instance for the same requests")
                .isSameAs(elementWithSameUuid.get());
        verify(remote, only()).uuid();
    }

    @Test
    public void cachesPreviouslySeenUuids() throws InterruptedException, ExecutionException, TimeoutException {
        UUID uuid = UUID.randomUUID();
        ClientAwarePingableLeader remote = remoteWithUuid(uuid);
        GetSuspectedLeaderWithUuid function = functionForRemotes(remote);

        TestBatchElement firstRequest = TestBatchElement.of(uuid);
        function.accept(ImmutableList.of(firstRequest));

        assertThat(firstRequest.get()).contains(remote);
        verify(remote, times(1)).uuid();

        TestBatchElement secondRequest = TestBatchElement.of(uuid);
        function.accept(ImmutableList.of(secondRequest));

        assertThat(secondRequest.get()).contains(remote);
        verifyNoMoreInteractions(remote);
    }

    private static ClientAwarePingableLeader remoteWithUuid(UUID uuid) {
        ClientAwarePingableLeader mock = mock(ClientAwarePingableLeader.class);
        return when(mock.uuid()).thenReturn(uuid).getMock();
    }

    private static ClientAwarePingableLeader remoteWithRandomUuid() {
        return remoteWithUuid(UUID.randomUUID());
    }

    private GetSuspectedLeaderWithUuid functionForRemotes(ClientAwarePingableLeader... remotes) {
        Map<ClientAwarePingableLeader, ExecutorService> executors =
                Maps.toMap(ImmutableList.copyOf(remotes), $ -> executorService);

        return new GetSuspectedLeaderWithUuid(executors, LOCAL_UUID, Duration.ofSeconds(1));
    }

    @Value.Immutable
    interface TestBatchElement extends BatchElement<UUID, Optional<ClientAwarePingableLeader>> {

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
        default SettableFuture<Optional<ClientAwarePingableLeader>> result() {
            return SettableFuture.create();
        }

        default Optional<ClientAwarePingableLeader> get()
                throws InterruptedException, ExecutionException, TimeoutException {
            return result().get(1, TimeUnit.SECONDS);
        }

    }
}
