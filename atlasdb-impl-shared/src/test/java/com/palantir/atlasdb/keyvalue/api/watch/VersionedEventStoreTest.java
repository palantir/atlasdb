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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public final class VersionedEventStoreTest {

    private static final LockWatchEvent EVENT_1 =
            UnlockEvent.builder(ImmutableSet.of()).build(1L);
    private static final LockWatchEvent EVENT_2 =
            UnlockEvent.builder(ImmutableSet.of()).build(2L);
    private static final LockWatchEvent EVENT_3 =
            UnlockEvent.builder(ImmutableSet.of()).build(3L);
    private static final LockWatchEvent EVENT_4 =
            UnlockEvent.builder(ImmutableSet.of()).build(4L);

    private static final Sequence SEQ_MIN = Sequence.of(-1L);
    private static final Sequence SEQ_1 = Sequence.of(1L);
    private static final Sequence SEQ_2 = Sequence.of(2L);
    private static final Sequence SEQ_3 = Sequence.of(3L);
    private static final Sequence SEQ_4 = Sequence.of(4L);
    private static final CacheMetrics CACHE_METRICS = CacheMetrics.create(MetricsManagers.createForTests());

    private VersionedEventStore eventStore;

    @Before
    public void before() {
        eventStore = new VersionedEventStore(2, 20, CACHE_METRICS);
    }

    @Test
    public void retentionEventsDoesNotRetentionAfterEarliestVersion() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        LockWatchEvents emptyEvents = eventStore.retentionEvents(Optional.of(SEQ_MIN));
        assertThat(emptyEvents.events()).isEmpty();
        assertThat(eventStore.getStateForTesting().eventMap().keySet())
                .containsExactlyInAnyOrder(SEQ_1, SEQ_2, SEQ_3, SEQ_4);

        LockWatchEvents events = eventStore.retentionEvents(Optional.empty());
        assertThat(events.events().stream().map(LockWatchEvent::sequence)).containsExactly(1L, 2L);
        assertThat(eventStore.getStateForTesting().eventMap().firstKey()).isEqualTo(SEQ_3);
    }

    @Test
    public void containsReturnsTrueForValuesLargerThanFirstKey() {
        eventStore.putAll(makeEvents(EVENT_4));
        assertThat(eventStore.containsEntryLessThanOrEqualTo(1L)).isFalse();
        assertThat(eventStore.containsEntryLessThanOrEqualTo(5L)).isTrue();
    }

    @Test
    public void getEventsBetweenVersionsReturnsInclusiveOnBounds() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.getEventsBetweenVersionsInclusive(Optional.of(2L), 3L))
                .containsExactly(EVENT_2, EVENT_3);
    }

    @Test
    public void getEventsBetweenVersionsStartsFromFirstKeyIfNotSpecified() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.getEventsBetweenVersionsInclusive(Optional.empty(), 3L))
                .containsExactly(EVENT_1, EVENT_2, EVENT_3);
    }

    @Test
    public void retentionEventsClearsEventsOverMaxBound() {
        eventStore = new VersionedEventStore(1, 3, CACHE_METRICS);
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.retentionEvents(Optional.of(SEQ_MIN)).events().stream()
                        .map(LockWatchEvent::sequence))
                .as("First event retentioned anyway as it exceeds maximum size")
                .containsExactly(1L);
        assertThat(eventStore.retentionEvents(Optional.empty()).events().stream()
                        .map(LockWatchEvent::sequence))
                .as("Two more events retentioned as they exceed the minimum size")
                .containsExactly(2L, 3L);
    }

    private LockWatchEvents makeEvents(LockWatchEvent... events) {
        return LockWatchEvents.builder().addEvents(events).build();
    }
}
