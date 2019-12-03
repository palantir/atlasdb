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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.immutables.value.Value;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.lock.watch.LockWatchEvent;

public class ArrayLockEventSlidingWindowTest {
    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(10);

    @Test
    public void whenLastKnownVersionIsAfterCurrentReturnEmpty() {
        int numEntries = 5;
        addEvents(numEntries);
        assertThat(slidingWindow.getFromVersion(numEntries + 1)).isEmpty();
    }

    @Test
    public void whenLastKnownVersionIsTooOldReturnEmpty() {
        int numEntries = 15;
        addEvents(numEntries);
        assertThat(slidingWindow.getFromVersion(2)).isEmpty();
    }

    @Test
    public void whenNoNewEventsReturnEmptyList() {
        int numEntries = 5;
        addEvents(numEntries);
        Optional<List<LockWatchEvent>> result = slidingWindow.getFromVersion(numEntries - 1);
        assertThat(result).contains(ImmutableList.of());
    }

    @Test
    public void returnConsecutiveRange() {
        int numEntries = 5;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(2, 3, numEntries - 1);
    }

    @Test
    public void returnWrappingRange() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(8, 9, numEntries - 1);
    }

    @Test
    public void returnWrappingRangeOnBoundary() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(9, 10, numEntries - 1);
    }

    @Test
    public void returnRangeAfterBoundary() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(10, 11, numEntries - 1);
    }

    private void addEvent() {
        slidingWindow.add(ArrayLockEventSlidingWindowTest::createEvent);
    }

    private void addEvents(int number) {
        for (int i = 0; i < number; i++) {
            addEvent();
        }
    }

    private void assertContainsEventsInOrderFromTo(long version, int startInclusive, int endInclusive) {
        Optional<List<LockWatchEvent>> result = slidingWindow.getFromVersion(version);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactlyElementsOf(
                LongStream.rangeClosed(startInclusive, endInclusive)
                        .boxed()
                        .map(ArrayLockEventSlidingWindowTest::createEvent)
                        .collect(Collectors.toList()));
    }

    private static LockWatchEvent createEvent(long sequence) {
        return ImmutableFakeLockWatchEvent.of(sequence);
    }

    @Value.Immutable
    abstract static class FakeLockWatchEvent implements LockWatchEvent {
        @Value.Parameter
        public abstract long sequence();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return null;
        }
    }
}
