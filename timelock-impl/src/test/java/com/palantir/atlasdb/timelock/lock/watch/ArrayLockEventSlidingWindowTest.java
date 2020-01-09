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

package com.palantir.atlasdb.timelock.lock.watch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.immutables.value.Value;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.lock.watch.ArrayLockEventSlidingWindow.EventsAndVersion;
import com.palantir.lock.watch.LockWatchEvent;

public class ArrayLockEventSlidingWindowTest {
    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(10);

    @Test
    public void getWithEmptyVersionReturnsEntireHistory() {
        int numEntries = 5;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(OptionalLong.empty(), 0, numEntries - 1);
    }

    @Test
    public void getAtLastVersionReturnsEmptyList() {
        int numEntries = 5;
        addEvents(numEntries);
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.of(numEntries - 1));
        assertThat(result.events()).hasValue(ImmutableList.of());
        assertThat(result.lastVersion()).hasValue(numEntries - 1);
    }

    @Test
    public void getAfterLastVersionReturnsEmpty() {
        int numEntries = 5;
        addEvents(numEntries);
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.of(numEntries + 1));
        assertThat(result.events()).isEmpty();
        assertThat(result.lastVersion()).hasValue(numEntries - 1);
    }

    @Test
    public void getWithVersionOutOfDateReturnsEmpty() {
        int numEntries = 15;
        addEvents(numEntries);
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.of(2));
        assertThat(result.events()).isEmpty();
        assertThat(result.lastVersion()).hasValue(numEntries - 1);
    }

    @Test
    public void getWithNoVersionReturnsEmptyWhenTooOld() {
        int numEntries = 15;
        addEvents(numEntries);
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.empty());
        assertThat(result.events()).isEmpty();
        assertThat(result.lastVersion()).hasValue(numEntries - 1);
    }

    @Test
    public void getFromMiddleOfHistory() {
        int numEntries = 5;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(OptionalLong.of(2), 3, numEntries - 1);
    }

    @Test
    public void getAcrossMaximumSizeBoundary() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(OptionalLong.of(8), 9, numEntries - 1);
    }

    @Test
    public void getAcrossMaximumSizeBoundaryStartingAtTheEnd() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(OptionalLong.of(9), 10, numEntries - 1);
    }

    @Test
    public void getFromVersionAfterMaximumBoundary() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(OptionalLong.of(10), 11, numEntries - 1);
    }

    @Test
    public void singleOversizedEventFitsInTheWindow() {
        slidingWindow.add(eventOfSize(20));
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.empty());
        assertThat(result.events()).contains(ImmutableList.of(eventOfSize(20).build(0)));
        assertThat(result.lastVersion()).isEqualTo(OptionalLong.of(0));
    }

    @Test
    public void oversizedEventEvictsPreviousEntries() {
        slidingWindow.add(eventOfSize(1));
        slidingWindow.add(eventOfSize(1));
        slidingWindow.add(eventOfSize(20));
        assertThat(slidingWindow.getFromVersion(OptionalLong.empty()).events()).isEmpty();
        assertThat(slidingWindow.getFromVersion(OptionalLong.of(0)).events()).isEmpty();
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.of(1));
        assertThat(result.events()).contains(ImmutableList.of(eventOfSize(20).build(2)));
        assertThat(result.lastVersion()).isEqualTo(OptionalLong.of(2));
    }

    @Test
    public void oversizedEventGetsEvictedImmediately() {
        slidingWindow.add(eventOfSize(20));
        slidingWindow.add(eventOfSize(1));
        assertThat(slidingWindow.getFromVersion(OptionalLong.empty()).events()).isEmpty();
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.of(0));
        assertThat(result.events()).contains(ImmutableList.of(eventOfSize(1).build(1)));
        assertThat(result.lastVersion()).isEqualTo(OptionalLong.of(1));
    }

    @Test
    public void windowEvictsExtraEventsAsNecessary() {
        slidingWindow.add(eventOfSize(4));
        slidingWindow.add(eventOfSize(5));
        slidingWindow.add(eventOfSize(6));
        assertThat(slidingWindow.getFromVersion(OptionalLong.empty()).events()).isEmpty();
        assertThat(slidingWindow.getFromVersion(OptionalLong.of(0)).events()).isEmpty();
        EventsAndVersion result = slidingWindow.getFromVersion(OptionalLong.of(1));
        assertThat(result.events()).contains(ImmutableList.of(eventOfSize(6).build(2)));
        assertThat(result.lastVersion()).isEqualTo(OptionalLong.of(2));
    }

    private void addEvent() {
        slidingWindow.add(ArrayLockEventSlidingWindowTest::createEvent);
    }

    private void addEvents(int number) {
        for (int i = 0; i < number; i++) {
            addEvent();
        }
    }

    private void assertContainsEventsInOrderFromTo(OptionalLong version, int startInclusive, int endInclusive) {
        Optional<List<LockWatchEvent>> result = slidingWindow.getFromVersion(version).events();
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactlyElementsOf(
                LongStream.rangeClosed(startInclusive, endInclusive)
                        .boxed()
                        .map(ArrayLockEventSlidingWindowTest::createEvent)
                        .collect(Collectors.toList()));
    }

    private static LockWatchEvent createEvent(long sequence) {
        return ImmutableFakeLockWatchEvent.of(sequence, 1);
    }

    private static LockWatchEvent.Builder eventOfSize(int size) {
        return sequence -> ImmutableFakeLockWatchEvent.of(sequence, size);

    }

    @Value.Immutable
    abstract static class FakeLockWatchEvent implements LockWatchEvent {
        @Value.Parameter
        public abstract long sequence();

        @Value.Parameter
        public abstract int size();

        @Override
        public <T> T accept(LockWatchEvent.Visitor<T> visitor) {
            // do nothing
            return null;
        }
    }
}
