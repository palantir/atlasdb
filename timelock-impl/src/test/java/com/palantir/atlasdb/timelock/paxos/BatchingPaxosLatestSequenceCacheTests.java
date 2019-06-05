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
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.conjure.java.api.errors.RemoteException;
import com.palantir.conjure.java.api.errors.SerializableError;
import com.palantir.conjure.java.api.errors.ServiceException;

@RunWith(MockitoJUnitRunner.class)
public class BatchingPaxosLatestSequenceCacheTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");
    private static final Client CLIENT_3 = Client.of("client-3");
    private static final Map<Client, Long> INITIAL_UPDATES = ImmutableMap.<Client, Long>builder()
            .put(CLIENT_1, 5L)
            .put(CLIENT_2, 10L)
            .put(CLIENT_3, 15L)
            .build();

    @Mock
    private BatchPaxosAcceptor remote;

    @Test
    public void withoutCacheKeyWeGetEverything() {
        AcceptorCacheDigest digest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .putUpdates(CLIENT_1, 5)
                .putUpdates(CLIENT_2, 10)
                .build();

        when(remote.latestSequencesPreparedOrAccepted(Optional.empty(), ImmutableSet.of(CLIENT_1, CLIENT_2)))
                .thenReturn(digest);

        BatchingPaxosLatestSequenceCache cache = new BatchingPaxosLatestSequenceCache(remote);
        Map<Client, Long> result = cache.apply(ImmutableSet.of(CLIENT_1, CLIENT_2));

        assertThat(result).containsOnly(
                entry(CLIENT_1, 5L),
                entry(CLIENT_2, 10L));
    }

    @Test
    public void returnsSameResultIfCached() {
        BatchingPaxosLatestSequenceCache cache = initialCache();
        when(remote.latestSequencesPreparedOrAcceptedCached(any(AcceptorCacheKey.class)))
                .thenReturn(Optional.empty());

        assertThat(cache.apply(ImmutableSet.of(CLIENT_1)))
                .containsEntry(CLIENT_1, INITIAL_UPDATES.get(CLIENT_1));

    }

    @Test
    public void ifThereAreUpdatesWithCacheKeyWeAddToOurCache() {
        BatchingPaxosLatestSequenceCache cache = initialCache();
        AcceptorCacheDigest digest = digestWithUpdates(entry(CLIENT_3, 50L));

        when(remote.latestSequencesPreparedOrAcceptedCached(any(AcceptorCacheKey.class)))
                .thenReturn(Optional.of(digest));

        assertThat(cache.apply(ImmutableSet.of(CLIENT_2, CLIENT_3)))
                .containsEntry(CLIENT_2, INITIAL_UPDATES.get(CLIENT_2))
                .containsEntry(CLIENT_3, 50L)
                .doesNotContainEntry(CLIENT_3, INITIAL_UPDATES.get(CLIENT_3));
    }

    @Test
    public void ifThereAreUpdatesWithCacheKeyWeAddToOurCache_unseenClient() {
        BatchingPaxosLatestSequenceCache cache = initialCache();
        Client client4 = Client.of("client-4");
        AcceptorCacheDigest digest = digestWithUpdates(entry(client4, 150L));

        when(remote.latestSequencesPreparedOrAccepted(any(Optional.class), eq(ImmutableSet.of(client4))))
                .thenReturn(digest);

        assertThat(cache.apply(ImmutableSet.of(CLIENT_3, client4)))
                .containsEntry(CLIENT_3, INITIAL_UPDATES.get(CLIENT_3))
                .containsEntry(client4, 150L);
    }

    @Test
    public void invalidCacheKeyRequestsEverything() {
        BatchingPaxosLatestSequenceCache cache = initialCache();

        when(remote.latestSequencesPreparedOrAcceptedCached(any(AcceptorCacheKey.class)))
                .thenThrow(invalidCacheKeyException());

        Map<Client, Long> newMap = ImmutableMap.<Client, Long>builder()
                .put(CLIENT_1, 52L)
                .put(CLIENT_2, 17L)
                .put(CLIENT_3, 1123L)
                .build();

        AcceptorCacheDigest newDigest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .putAllUpdates(newMap)
                .build();

        doReturn(newDigest).when(remote)
                .latestSequencesPreparedOrAccepted(Optional.empty(), ImmutableSet.of(CLIENT_1, CLIENT_2, CLIENT_3));

        assertThat(cache.apply(ImmutableSet.of(CLIENT_1)).get(CLIENT_1))
                .as("we should get 52 which results from calling the non cached version")
                .isEqualTo(52);
    }

    private BatchingPaxosLatestSequenceCache initialCache() {
        AcceptorCacheDigest digest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .putAllUpdates(INITIAL_UPDATES)
                .build();

        when(remote.latestSequencesPreparedOrAccepted(Optional.empty(), ImmutableSet.of(CLIENT_1, CLIENT_2, CLIENT_3)))
                .thenReturn(digest);

        BatchingPaxosLatestSequenceCache cache = new BatchingPaxosLatestSequenceCache(remote);
        cache.apply(ImmutableSet.of(CLIENT_1, CLIENT_2, CLIENT_3));
        return cache;
    }

    private static RemoteException invalidCacheKeyException() {
        ServiceException serviceException = new ServiceException(
                ErrorType.create(ErrorType.Code.NOT_FOUND, BatchingPaxosLatestSequenceCache.ERROR_NAME));

        return new RemoteException(SerializableError.forException(serviceException), 404);
    }

    private static AcceptorCacheDigest digestWithUpdates(Map.Entry<Client, Long>... entries) {
        return ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .updates(ImmutableMap.copyOf(ImmutableSet.copyOf(entries)))
                .build();
    }
}
