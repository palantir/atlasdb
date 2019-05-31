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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class AcceptorCacheImplTests {

    @Test
    public void unseenCacheKeyThrows() {
        AcceptorCache cache = cache(ImmutableMap.of());
        AcceptorCacheKey cacheKey = AcceptorCacheKey.newCacheKey();

        assertThatExceptionOfType(InvalidAcceptorCacheKeyException.class)
                .as("we've not seen this cache key before, so we throw")
                .isThrownBy(() -> cache.updatesSinceCacheKey(cacheKey));
    }

    @Test
    public void cacheKeyWithNoUpdatesReturnsEmpty() throws InvalidAcceptorCacheKeyException {
        Map<Client, Long> before = ImmutableMap.<Client, Long>builder()
                .put(Client.of("client1"), 5L)
                .put(Client.of("client2"), 6L)
                .put(Client.of("client3"), 7L)
                .build();
        AcceptorCache cache = cache(before);

        AcceptorCacheDigest digest = cache.getAllUpdates();

        assertThat(digest)
                .extracting(AcceptorCacheDigest::updates)
                .as("get all the updates")
                .isEqualTo(before);

        AcceptorCacheKey newCacheKey = digest.newCacheKey();
        assertThat(cache.updatesSinceCacheKey(newCacheKey))
                .as("we have not added anything after we asked for updates initially")
                .isEmpty();
    }

    @Test
    public void returnsLatestCacheKeyForOldCacheKeys() throws InvalidAcceptorCacheKeyException {
        AcceptorCache cache = cache(ImmutableMap.of());

        AcceptorCacheKey newCacheKey = cache.getAllUpdates().newCacheKey();

        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(20L, Client.of("client1"))));
        AcceptorCacheKey newCacheKeyAfterFirstUpdate = cache.getAllUpdates().newCacheKey();

        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(25L, Client.of("client4"))));
        AcceptorCacheKey newCacheKeyAfterSecondUpdate = cache.getAllUpdates().newCacheKey();

        assertThat(cache.updatesSinceCacheKey(newCacheKey))
                .map(AcceptorCacheDigest::newCacheKey)
                .as("asking with first cache key after two updates should return latest cache key")
                .contains(newCacheKeyAfterSecondUpdate);

        assertThat(cache.updatesSinceCacheKey(newCacheKeyAfterFirstUpdate))
                .map(AcceptorCacheDigest::newCacheKey)
                .as("asking with second cache key after one update should also return latest cache key")
                .contains(newCacheKeyAfterSecondUpdate);

        assertThat(cache.updatesSinceCacheKey(newCacheKeyAfterSecondUpdate))
                .as("no updates past this cache key as this is the latest cache key, return empty")
                .isEmpty();
    }

    @Test
    public void updatesAreCoalesced() throws InvalidAcceptorCacheKeyException {
        AcceptorCache cache = cache(ImmutableMap.of());

        AcceptorCacheKey cacheKey = cache.getAllUpdates().newCacheKey();
        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(20L, Client.of("client1"))));
        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(25L, Client.of("client4"))));

        Map<Client, Long> diffAfterFirstUpdate = ImmutableMap.<Client, Long>builder()
                .put(Client.of("client1"), 20L)
                .put(Client.of("client4"), 25L)
                .build();

        assertThat(cache.updatesSinceCacheKey(cacheKey))
                .map(AcceptorCacheDigest::updates)
                .as("we should see the two updates as normal")
                .contains(diffAfterFirstUpdate);

        cache.updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(30L, Client.of("client4")),
                WithSeq.of(50L, Client.of("client3"))));

        Map<Client, Long> diffAfterSecondUpdate = ImmutableMap.<Client, Long>builder()
                .put(Client.of("client1"), 20L)
                .put(Client.of("client4"), 30L)
                .put(Client.of("client3"), 50L)
                .build();

        assertThat(cache.updatesSinceCacheKey(cacheKey))
                .map(AcceptorCacheDigest::updates)
                .as("we should see the final value for client4=30 as opposed to the intermediate value")
                .contains(diffAfterSecondUpdate);
    }

    private static AcceptorCache cache(Map<Client, Long> latestSequencesForClients) {
        AcceptorCacheImpl acceptorCache = new AcceptorCacheImpl();

        Set<WithSeq<Client>> clientsAndSeq = latestSequencesForClients.entrySet().stream()
                .map(clientAndSeq -> WithSeq.of(clientAndSeq.getValue(), clientAndSeq.getKey()))
                .collect(Collectors.toSet());
        acceptorCache.updateSequenceNumbers(clientsAndSeq);
        return acceptorCache;
    }
}
