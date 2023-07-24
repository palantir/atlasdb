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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchEvent;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.Test;

public class ArrayLockEventSlidingWindowTest {
    private static final int WINDOW_SIZE = 10;

    private static final LockDescriptor LOCK_1 = StringLockDescriptor.of("abc");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("def");
    private static final LockDescriptor LOCK_3 = StringLockDescriptor.of("ghi");
    private static final LockToken TOKEN = LockToken.of(UUID.randomUUID());

    private final BufferMetrics bufferMetrics =
            BufferMetrics.of(MetricsManagers.createForTests().getTaggedRegistry());
    private final ArrayLockEventSlidingWindow slidingWindow =
            new ArrayLockEventSlidingWindow(WINDOW_SIZE, bufferMetrics);

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

    @Test
    public void countsMetadataCorrectlyStartingFromEmptyBuffer() {
        LockRequestMetadata metadata = LockRequestMetadata.of(ImmutableMap.of(
                LOCK_1, ChangeMetadata.unchanged(), LOCK_2, ChangeMetadata.created(PtBytes.toBytes("foo"))));
        LockRequestMetadata metadata2 =
                LockRequestMetadata.of(ImmutableMap.of(LOCK_3, ChangeMetadata.deleted(PtBytes.toBytes("bar"))));
        addLockEventWithMetadata(ImmutableSet.of(LOCK_1, LOCK_2), Optional.of(metadata));
        addLockEventWithMetadata(ImmutableSet.of(LOCK_3), Optional.of(metadata2));

        assertThat(bufferMetrics.changeMetadata().getCount()).isEqualTo(3);
        assertThat(bufferMetrics.eventsWithMetadata().getCount()).isEqualTo(2);

        addLockEventWithMetadata(ImmutableSet.of(LOCK_1), Optional.empty());
        addLockEventWithMetadata(ImmutableSet.of(), Optional.of(LockRequestMetadata.of(ImmutableMap.of())));

        assertThat(bufferMetrics.changeMetadata().getCount())
                .as("Absent metadata should count for nothing")
                .isEqualTo(3);
        assertThat(bufferMetrics.eventsWithMetadata().getCount())
                .as("Empty metadata map should count for present metadata")
                .isEqualTo(3);
    }

    @Test
    public void maintainsCorrectMetadataCountWhenOverwritingBuffer() {
        assertThat(WINDOW_SIZE)
                .as("This test does not work with small windows since we are going to replace multiple events once"
                        + " our buffer is full. With small windows, we might overwrite the wrong events.")
                .isGreaterThanOrEqualTo(5);

        IntStream.range(0, WINDOW_SIZE).forEach(i -> {
            LockDescriptor lock = StringLockDescriptor.of("lock" + i);
            LockRequestMetadata metadata = LockRequestMetadata.of(
                    ImmutableMap.of(lock, ChangeMetadata.created(PtBytes.toBytes("created" + i))));
            addLockEventWithMetadata(ImmutableSet.of(lock), Optional.of(metadata));
        });
        assertThat(bufferMetrics.changeMetadata().getCount()).isEqualTo(WINDOW_SIZE);
        assertThat(bufferMetrics.eventsWithMetadata().getCount()).isEqualTo(WINDOW_SIZE);

        // Counts are maintained if we replace an event with metadata of same size
        addLockEventWithMetadata(
                ImmutableSet.of(LOCK_1),
                Optional.of(LockRequestMetadata.of(ImmutableMap.of(LOCK_1, ChangeMetadata.unchanged()))));
        assertThat(bufferMetrics.changeMetadata().getCount()).isEqualTo(WINDOW_SIZE);
        assertThat(bufferMetrics.eventsWithMetadata().getCount()).isEqualTo(WINDOW_SIZE);

        // Both counts can decrease
        addLockEventWithMetadata(ImmutableSet.of(LOCK_1), Optional.empty());
        assertThat(bufferMetrics.changeMetadata().getCount()).isEqualTo(WINDOW_SIZE - 1);
        assertThat(bufferMetrics.eventsWithMetadata().getCount()).isEqualTo(WINDOW_SIZE - 1);

        // ChangeMetadata count can increase
        addLockEventWithMetadata(
                ImmutableSet.of(LOCK_1, LOCK_2, LOCK_3),
                Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                        LOCK_1, ChangeMetadata.created(PtBytes.toBytes("created")),
                        LOCK_2, ChangeMetadata.unchanged(),
                        LOCK_3, ChangeMetadata.updated(PtBytes.toBytes("old"), PtBytes.toBytes("new"))))));
        assertThat(bufferMetrics.changeMetadata().getCount()).isEqualTo(WINDOW_SIZE + 1);
        assertThat(bufferMetrics.eventsWithMetadata().getCount()).isEqualTo(WINDOW_SIZE - 1);
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
        assertThat(result)
                .containsExactlyElementsOf(LongStream.rangeClosed(startInclusive, endInclusive)
                        .boxed()
                        .map(ArrayLockEventSlidingWindowTest::createEvent)
                        .collect(Collectors.toList()));
    }

    private void addLockEventWithMetadata(Set<LockDescriptor> lockDescriptors, Optional<LockRequestMetadata> metadata) {
        slidingWindow.add(LockEvent.builder(lockDescriptors, TOKEN, metadata));
    }

    private static LockWatchEvent createEvent(long sequence) {
        return LockEvent.builder(ImmutableSet.of(), TOKEN).build(sequence);
    }
}
