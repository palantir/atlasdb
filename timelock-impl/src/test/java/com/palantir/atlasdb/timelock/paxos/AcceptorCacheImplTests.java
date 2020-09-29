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
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

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
    public void updatingWithNonRecentValueDoesNotAffectCache() throws InvalidAcceptorCacheKeyException {
        Map<Client, Long> before = ImmutableMap.<Client, Long>builder()
                .put(Client.of("client1"), 5L)
                .put(Client.of("client2"), 6L)
                .put(Client.of("client3"), 7L)
                .build();
        AcceptorCache cache = cache(before);
        AcceptorCacheDigest digest = cache.getAllUpdates();

        cache.updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(Client.of("client1"), 10L),
                WithSeq.of(Client.of("client2"), 3L)));

        AcceptorCacheDigest secondDigest = cache.updatesSinceCacheKey(digest.newCacheKey()).get();
        assertThat(secondDigest.updates())
                .containsOnly(entry(Client.of("client1"), 10L));

        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(Client.of("client3"), 5L)));

        assertThat(cache.updatesSinceCacheKey(secondDigest.newCacheKey()))
                .as("there are no actionable changes, cache is the same")
                .isEmpty();

        assertThat(cache.updatesSinceCacheKey(digest.newCacheKey()))
                .as("no actionable changes => this is no different from calling updatesSinceCacheKey twice")
                .contains(secondDigest);
    }

    @Test
    public void returnsLatestCacheKeyForOldCacheKeys() throws InvalidAcceptorCacheKeyException {
        AcceptorCache cache = cache(ImmutableMap.of());

        AcceptorCacheKey initialCacheKey = cache.getAllUpdates().newCacheKey();

        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(Client.of("client1"), 20L)));
        AcceptorCacheKey cacheKeyAfterFirstUpdate = cache.getAllUpdates().newCacheKey();

        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(Client.of("client4"), 25L)));
        AcceptorCacheKey cacheKeyAfterSecondUpdate = cache.getAllUpdates().newCacheKey();

        assertThat(cache.updatesSinceCacheKey(initialCacheKey))
                .map(AcceptorCacheDigest::newCacheKey)
                .as("asking with first cache key after two updates should return latest cache key")
                .contains(cacheKeyAfterSecondUpdate);

        assertThat(cache.updatesSinceCacheKey(cacheKeyAfterFirstUpdate))
                .map(AcceptorCacheDigest::newCacheKey)
                .as("asking with second cache key after one update should also return latest cache key")
                .contains(cacheKeyAfterSecondUpdate);

        assertThat(cache.updatesSinceCacheKey(cacheKeyAfterSecondUpdate))
                .as("no updates past this cache key as this is the latest cache key, return empty")
                .isEmpty();
    }

    @Test
    public void updatesAreCoalesced() throws InvalidAcceptorCacheKeyException {
        AcceptorCache cache = cache(ImmutableMap.of());

        AcceptorCacheKey cacheKey = cache.getAllUpdates().newCacheKey();
        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(Client.of("client1"), 20L)));
        cache.updateSequenceNumbers(ImmutableSet.of(WithSeq.of(Client.of("client4"), 25L)));

        Map<Client, Long> diffAfterFirstUpdate = ImmutableMap.<Client, Long>builder()
                .put(Client.of("client1"), 20L)
                .put(Client.of("client4"), 25L)
                .build();

        assertThat(cache.updatesSinceCacheKey(cacheKey))
                .map(AcceptorCacheDigest::updates)
                .as("we should see the two updates as normal")
                .contains(diffAfterFirstUpdate);

        cache.updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(Client.of("client4"), 30L),
                WithSeq.of(Client.of("client3"), 50L)));

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

        Set<WithSeq<Client>> clientsAndSeq = KeyedStream.stream(latestSequencesForClients)
                .map(WithSeq::of)
                .values()
                .collect(Collectors.toSet());
        acceptorCache.updateSequenceNumbers(clientsAndSeq);
        return acceptorCache;
    }
}
