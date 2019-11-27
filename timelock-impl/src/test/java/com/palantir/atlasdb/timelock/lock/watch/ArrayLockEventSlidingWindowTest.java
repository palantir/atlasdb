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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.immutables.value.Value;
import org.junit.Test;

import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventVisitor;

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
    public void returnConsecutiveRange() {
        int numEntries = 5;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(2, 2, numEntries - 1);
    }

    @Test
    public void returnWrappingRange() {
        int numEntries = 15;
        addEvents(numEntries);
        assertContainsEventsInOrderFromTo(8, 8, numEntries - 1);
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
        List<LockWatchEvent> result = slidingWindow.getFromVersion(version);
        assertThat(result).containsExactlyElementsOf(
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
        public void accept(LockWatchEventVisitor visitor) {
            // do nothing
        }
    }
}
