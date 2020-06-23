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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.immutables.value.Value;
import org.junit.Test;

import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public class ArrayLockEventSlidingWindowTest {
    private static final int WINDOW_SIZE = 10;

    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(WINDOW_SIZE);

    @Test
    public void whenLastKnownVersionIsAfterCurrentThrows() {
        int numEntries = 5;
        addEvents(numEntries);
        // Log contains events [0,1,2,3,4]
        assertThatLoggableExceptionThrownBy(() -> slidingWindow.getNextEvents(numEntries))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Version not in the log");
    }

    @Test
    public void whenLastKnownVersionIsTooOldThrows() {
        addEvents(WINDOW_SIZE + 5);
        // Added events [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]
        // Log contains [5,6,7,8,9,10,11,12,13,14]
        assertThatLoggableExceptionThrownBy(() -> slidingWindow.getNextEvents(3))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Version not in the log");
    }

    @Test
    public void whenNoNewEventsReturnEmptyList() {
        int numEntries = 5;
        addEvents(numEntries);
        List<LockWatchEvent> result = slidingWindow.getNextEvents(numEntries - 1);
        assertThat(result).isEmpty();
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
        List<LockWatchEvent> result = slidingWindow.getNextEvents(version);
        assertThat(result).containsExactlyElementsOf(
                LongStream.rangeClosed(startInclusive, endInclusive)
                        .boxed()
                        .map(ArrayLockEventSlidingWindowTest::createEvent)
                        .collect(Collectors.toList()));
    }

    private static LockWatchEvent createEvent(long sequence) {
        return ImmutableFakeLockWatchEvent.of(sequence, 1);
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
