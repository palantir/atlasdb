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

import com.palantir.lock.watch.LockWatchEvent;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.immutables.value.Value;
import org.junit.Test;

public class ArrayLockEventSlidingWindowTest {
    private static final int WINDOW_SIZE = 10;

    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(WINDOW_SIZE);

    @Test
    public void whenLastKnownVersionIsAfterCurrentReturnEmpty() {
        whenLogContainsEvents0To4();
        assertThat(slidingWindow.getNextEvents(5)).isEmpty();
    }

    @Test
    public void whenLastKnownVersionIsTooOldReturnEmpty() {
        whenLogContainsEvents5to14();
        assertThat(slidingWindow.getNextEvents(3)).isEmpty();
    }

    @Test
    public void whenNoNewEventsReturnEmptyList() {
        whenLogContainsEvents0To4();
        assertThat(slidingWindow.getNextEvents(4).get()).isEmpty();
    }

    @Test
    public void returnConsecutiveRange() {
        whenLogContainsEvents0To4();
        assertContainsNextEventsInOrder(2, 3, 4);
    }

    @Test
    public void returnWrappingRange() {
        whenLogContainsEvents5to14();
        assertContainsNextEventsInOrder(8, 9, 14);
    }

    @Test
    public void returnWrappingRangeOnBoundary() {
        whenLogContainsEvents5to14();
        assertContainsNextEventsInOrder(9, 10, 14);
    }

    @Test
    public void returnRangeAfterBoundary() {
        whenLogContainsEvents5to14();
        assertContainsNextEventsInOrder(10, 11, 14);
    }

    private void whenLogContainsEvents0To4() {
        // Log contains events [0,1,2,3,4]
        addEvents(5);
    }

    private void whenLogContainsEvents5to14() {
        // Log contains events [5,6,7,8,9,10,11,12,13,14]
        addEvents(15);
    }

    private void addEvent() {
        slidingWindow.add(ArrayLockEventSlidingWindowTest::createEvent);
    }

    private void addEvents(int number) {
        for (int i = 0; i < number; i++) {
            addEvent();
        }
    }

    private void assertContainsNextEventsInOrder(long version, int startInclusive, int endInclusive) {
        List<LockWatchEvent> result = slidingWindow.getNextEvents(version).get();
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
