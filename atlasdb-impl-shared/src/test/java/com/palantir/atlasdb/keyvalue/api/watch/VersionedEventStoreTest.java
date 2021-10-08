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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
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
    private static final LockWatchEvent EVENT_5 =
            UnlockEvent.builder(ImmutableSet.of()).build(5L);

    private static final Sequence SEQ_MIN = Sequence.of(-1L);
    private static final Sequence SEQ_1 = Sequence.of(1L);
    private static final Sequence SEQ_2 = Sequence.of(2L);
    private static final Sequence SEQ_3 = Sequence.of(3L);
    private static final Sequence SEQ_4 = Sequence.of(4L);
    private static final CacheMetrics CACHE_METRICS = CacheMetrics.create(MetricsManagers.createForTests());

    private static final int MIN_EVENTS = 2;
    private static final int MAX_EVENTS = 20;

    private VersionedEventStore eventStore;

    @Before
    public void before() {
        eventStore = new VersionedEventStore(CACHE_METRICS, MIN_EVENTS, MAX_EVENTS);
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
    public void getEventsBetweenVersionsReturnsNothingIfStartIsAfterLastEvent() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.getEventsBetweenVersionsInclusive(Optional.of(9L), 11L))
                .isEmpty();
    }

    @Test
    public void getEventsBetweenVersionsDoesNotIncludeFirstKeyIfStartNotSpecifiedAndEndVersionPrecedesIt() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.getEventsBetweenVersionsInclusive(Optional.empty(), 0L))
                .isEmpty();
    }

    @Test
    public void getEventsBetweenVersionsReturnsAllEventsWhenQueriedPastEnd() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.getEventsBetweenVersionsInclusive(Optional.empty(), Long.MAX_VALUE))
                .containsExactly(EVENT_1, EVENT_2, EVENT_3, EVENT_4);
    }

    @Test
    public void getEventsBetweenVersionsReturnsEmptyForEmptyEventStore() {
        assertThat(eventStore.getEventsBetweenVersionsInclusive(Optional.empty(), Long.MAX_VALUE))
                .isEmpty();
    }

    @Test
    public void containsEntryLessThanOrEqualToOperatesCorrectlyForFilledStore() {
        eventStore.putAll(makeEvents(EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.containsEntryLessThanOrEqualTo(1L)).isFalse();
        assertThat(eventStore.containsEntryLessThanOrEqualTo(2L)).isTrue();
        assertThat(eventStore.containsEntryLessThanOrEqualTo(5L)).isTrue();
        assertThat(eventStore.containsEntryLessThanOrEqualTo(Long.MAX_VALUE)).isTrue();
    }

    @Test
    public void containsEntryLessThanOrEqualToOperatesCorrectlyForEmptyStore() {
        assertThat(eventStore.containsEntryLessThanOrEqualTo(Long.MIN_VALUE)).isFalse();
        assertThat(eventStore.containsEntryLessThanOrEqualTo(Long.MAX_VALUE)).isFalse();
    }

    @Test
    // TODO (jkong): If we don't want to support this case, we should explicitly reject it rather than relying on an
    //  SISE.
    public void puttingNoEventsThrowsException() {
        assertThatThrownBy(() -> eventStore.putAll(makeEvents()))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage("Cannot get last key from empty map");
    }

    @Test
    public void putAllReturnsLastIndexAfterAddition() {
        assertThat(eventStore.putAll(makeEvents(EVENT_1))).isEqualTo(EVENT_1.sequence());
        assertThat(eventStore.putAll(makeEvents(EVENT_2, EVENT_3))).isEqualTo(EVENT_3.sequence());
        assertThat(eventStore.putAll(makeEvents(EVENT_4))).isEqualTo(EVENT_4.sequence());
    }

    @Test
    public void retentionDoesNotClearEventsBelowMinimumSize() {
        eventStore.putAll(makeEvents(EVENT_1));
        LockWatchEvents events = eventStore.retentionEvents(Optional.empty());
        assertThat(events.events()).isEmpty();
        assertThat(events.versionRange()).isEmpty();
        assertThat(eventStore.getStateForTesting().eventMap().keySet()).containsExactlyInAnyOrder(SEQ_1);
    }

    @Test
    public void retentionEventsDoesNotRetentionEventsFromEarliestVersionToKeep() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        LockWatchEvents eventsRetentionedBeforeMinSeq = eventStore.retentionEvents(Optional.of(SEQ_MIN));
        assertThat(eventsRetentionedBeforeMinSeq.events()).isEmpty();
        assertThat(eventStore.getStateForTesting().eventMap().keySet())
                .containsExactlyInAnyOrder(SEQ_1, SEQ_2, SEQ_3, SEQ_4);

        LockWatchEvents eventsRetentionedBeforeFirstSeq = eventStore.retentionEvents(Optional.of(SEQ_1));
        assertThat(eventsRetentionedBeforeFirstSeq.events()).isEmpty();
        assertThat(eventStore.getStateForTesting().eventMap().keySet())
                .containsExactlyInAnyOrder(SEQ_1, SEQ_2, SEQ_3, SEQ_4);
    }

    @Test
    public void retentionEventsRetentionsEventsUntilMinThreshold() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        LockWatchEvents events = eventStore.retentionEvents(Optional.empty());

        assertThat(events.events().stream().map(LockWatchEvent::sequence).map(Sequence::of))
                .containsExactly(SEQ_1, SEQ_2);
        assertThat(events.versionRange()).contains(Range.closed(EVENT_1.sequence(), EVENT_2.sequence()));
        assertThat(eventStore.getStateForTesting().eventMap().firstKey()).isEqualTo(SEQ_3);
    }

    @Test
    public void retentionEventsRetentionsEventsUpToEarliestVersionToKeep() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        LockWatchEvents events = eventStore.retentionEvents(Optional.of(SEQ_2));

        assertThat(events.events().stream().map(LockWatchEvent::sequence).map(Sequence::of))
                .containsExactly(SEQ_1);
        assertThat(events.versionRange()).contains(Range.closed(EVENT_1.sequence(), EVENT_1.sequence()));
        assertThat(eventStore.getStateForTesting().eventMap().firstKey()).isEqualTo(SEQ_2);
    }

    @Test
    public void retentionEventsClearsEventsSuccessively() {
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4, EVENT_5));
        LockWatchEvents events = eventStore.retentionEvents(Optional.of(SEQ_2));

        assertThat(events.events().stream().map(LockWatchEvent::sequence).map(Sequence::of))
                .containsExactly(SEQ_1);
        assertThat(events.versionRange()).contains(Range.closed(EVENT_1.sequence(), EVENT_1.sequence()));
        assertThat(eventStore.getStateForTesting().eventMap().firstKey()).isEqualTo(SEQ_2);

        LockWatchEvents newEvents = eventStore.retentionEvents(Optional.of(SEQ_4));
        assertThat(newEvents.events().stream().map(LockWatchEvent::sequence).map(Sequence::of))
                .containsExactly(SEQ_2, SEQ_3);
        assertThat(eventStore.getStateForTesting().eventMap().firstKey()).isEqualTo(SEQ_4);
    }

    @Test
    public void retentionEventsClearsEventsAboveMaxSizeRegardlessOfVersion() {
        eventStore = new VersionedEventStore(CACHE_METRICS, 1, 3);
        eventStore.putAll(makeEvents(EVENT_1, EVENT_2, EVENT_3, EVENT_4));
        assertThat(eventStore.retentionEvents(Optional.of(SEQ_MIN)).events().stream()
                        .map(LockWatchEvent::sequence)
                        .map(Sequence::of))
                .as("First event retentioned as it exceeds maximum size; second and third are not, because while they"
                        + " exceed the minimum size they have a newer version.")
                .containsExactly(SEQ_1);
        assertThat(eventStore.retentionEvents(Optional.empty()).events().stream()
                        .map(LockWatchEvent::sequence)
                        .map(Sequence::of))
                .as("Two more events retentioned as they exceed the minimum size")
                .containsExactly(SEQ_2, SEQ_3);
        assertThat(eventStore.containsEntryLessThanOrEqualTo(SEQ_4.value())).isTrue();
    }

    private LockWatchEvents makeEvents(LockWatchEvent... events) {
        return LockWatchEvents.builder().addEvents(events).build();
    }
}
