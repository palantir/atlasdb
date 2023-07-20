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
import com.palantir.atlasdb.timelock.lockwatches.CurrentMetrics;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.UnlockEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ArrayLockEventSlidingWindow {
    private final LockWatchEvent[] buffer;
    private final int maxSize;
    private long nextSequence = 0;
    private final Counter changeMetadataCounter;
    private final Counter eventsWithMetadataCounter;

    ArrayLockEventSlidingWindow(int maxSize, CurrentMetrics metadataMetrics) {
        this.buffer = new LockWatchEvent[maxSize];
        this.maxSize = maxSize;
        this.changeMetadataCounter = metadataMetrics.changeMetadata();
        this.eventsWithMetadataCounter = metadataMetrics.eventsWithMetadata();
    }

    long lastVersion() {
        return nextSequence - 1;
    }

    void add(LockWatchEvent.Builder eventBuilder) {
        LockWatchEvent event = eventBuilder.build(nextSequence);
        int index = LongMath.mod(nextSequence, maxSize);
        updateCurrentMetadataMetrics(
                event.accept(LockWatchEventMetadataVisitor.INSTANCE),
                Optional.ofNullable(buffer[index])
                        .flatMap(replacedEvent -> replacedEvent.accept(LockWatchEventMetadataVisitor.INSTANCE)));
        buffer[index] = event;
        nextSequence++;
    }

    private void updateCurrentMetadataMetrics(
            Optional<LockRequestMetadata> newMetadata, Optional<LockRequestMetadata> replacedMetadata) {
        int changeMetadataSizeDiff = newMetadata
                        .map(LockRequestMetadata::lockDescriptorToChangeMetadata)
                        .map(Map::size)
                        .orElse(0)
                - replacedMetadata
                        .map(LockRequestMetadata::lockDescriptorToChangeMetadata)
                        .map(Map::size)
                        .orElse(0);
        int numPresentMetadataDiff = newMetadata.map(_unused -> 1).orElse(0)
                - replacedMetadata.map(_unused -> 1).orElse(0);
        changeMetadataCounter.inc(changeMetadataSizeDiff);
        eventsWithMetadataCounter.inc(numPresentMetadataDiff);
    }

    public Optional<List<LockWatchEvent>> getNextEvents(long version) {
        if (versionInTheFuture(version) || versionTooOld(version)) {
            return Optional.empty();
        }
        int startIndex = LongMath.mod(version + 1, maxSize);
        int windowSize = Ints.saturatedCast(lastVersion() - version);
        List<LockWatchEvent> events = new ArrayList<>(windowSize);

        for (int i = startIndex; events.size() < windowSize; i = incrementAndMod(i)) {
            events.add(buffer[i]);
        }

        return Optional.of(events);
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
