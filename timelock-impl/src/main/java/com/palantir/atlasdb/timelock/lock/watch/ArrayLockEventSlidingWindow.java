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

import com.codahale.metrics.Counter;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.UnlockEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ArrayLockEventSlidingWindow implements LockEventStore {
    private final LockWatchEvent[] buffer;
    private final int maxSize;
    private long nextSequence = 0;
    private final Counter changeMetadataCounter;
    private final Counter eventsWithMetadataCounter;

    ArrayLockEventSlidingWindow(int maxSize, BufferMetrics bufferMetrics) {
        this.buffer = new LockWatchEvent[maxSize];
        this.maxSize = maxSize;
        this.changeMetadataCounter = bufferMetrics.changeMetadata();
        this.eventsWithMetadataCounter = bufferMetrics.eventsWithMetadata();
    }

    @Override
    public synchronized long lastVersion() {
        return nextSequence - 1;
    }

    // This method is only for one-off diagnostics purposes.
    @Override
    public synchronized LockWatchEvent[] getBufferSnapshot() {
        // The contents of the buffer are immutable, thus not requiring a deep copy.
        return Arrays.copyOf(buffer, buffer.length);
    }

    @Override
    public synchronized void add(LockWatchEvent.Builder eventBuilder) {
        LockWatchEvent event = eventBuilder.build(nextSequence);
        int index = LongMath.mod(nextSequence, maxSize);

        Optional.ofNullable(buffer[index])
                .flatMap(replacedEvent -> replacedEvent.accept(LockWatchEventMetadataVisitor.INSTANCE))
                .ifPresent(this::decrementMetadataCounters);
        event.accept(LockWatchEventMetadataVisitor.INSTANCE).ifPresent(this::incrementMetadataCounters);

        buffer[index] = event;
        nextSequence++;
    }

    private void incrementMetadataCounters(LockRequestMetadata metadata) {
        changeMetadataCounter.inc(metadata.lockDescriptorToChangeMetadata().size());
        eventsWithMetadataCounter.inc();
    }

    private void decrementMetadataCounters(LockRequestMetadata metadata) {
        changeMetadataCounter.dec(metadata.lockDescriptorToChangeMetadata().size());
        eventsWithMetadataCounter.dec();
    }

    @Override
    public synchronized Optional<NextEvents> getNextEvents(long version) {
        if (versionInTheFuture(version) || versionTooOld(version)) {
            return Optional.empty();
        }
        int startIndex = LongMath.mod(version + 1, maxSize);
        int windowSize = Ints.saturatedCast(lastVersion() - version);
        List<LockWatchEvent> events = new ArrayList<>(windowSize);

        for (int i = startIndex; events.size() < windowSize; i = incrementAndMod(i)) {
            events.add(buffer[i]);
        }

        return Optional.of(new NextEvents(events, lastVersion()));
    }

    @Override
    public int capacity() {
        return buffer.length;
    }

    private int incrementAndMod(int num) {
        num++;
        return num >= maxSize ? num % maxSize : num;
    }

    private boolean versionInTheFuture(long version) {
        return version > lastVersion();
    }

    private boolean versionTooOld(long version) {
        return lastVersion() - version > maxSize;
    }

    private enum LockWatchEventMetadataVisitor implements LockWatchEvent.Visitor<Optional<LockRequestMetadata>> {
        INSTANCE;

        @Override
        public Optional<LockRequestMetadata> visit(LockEvent lockEvent) {
            return lockEvent.metadata();
        }

        @Override
        public Optional<LockRequestMetadata> visit(UnlockEvent unlockEvent) {
            return Optional.empty();
        }

        @Override
        public Optional<LockRequestMetadata> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return Optional.empty();
        }
    }
}
