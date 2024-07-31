/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.lmax.disruptor;

import com.codahale.metrics.Counter;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.lock.watch.LockEventStore;
import com.palantir.atlasdb.timelock.lock.watch.NextEvents;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RingBufferLockEventStore implements LockEventStore {

    private static final SafeLogger log = SafeLoggerFactory.get(RingBufferLockEventStore.class);

    public static final int BUFFER_SIZE = 1024;
    private final MultiProducerSequencer sequencer;
    private final RingBuffer<RingBufferElement> ringBuffer;
    private final Counter changeMetadataCounter;
    private final Counter eventsWithMetadataCounter;

    public RingBufferLockEventStore(int bufferSize, BufferMetrics bufferMetrics) {
        sequencer = new MultiProducerSequencer(bufferSize, new BlockingWaitStrategy());
        ringBuffer = new RingBuffer<>(RingBufferElement::new, sequencer);
        this.changeMetadataCounter = bufferMetrics.changeMetadata();
        this.eventsWithMetadataCounter = bufferMetrics.eventsWithMetadata();
    }

    @Override
    public long lastVersion() {
        // Need to go backwards from current cursor
        long cursor = ringBuffer.getCursor();
        if (cursor == AbstractSequencer.INITIAL_CURSOR_VALUE) {
            return -1;
        }
        // Let's not go down to zero, shouldn't be possible
        long minCursor = Math.max(0, getSmallestSequence(cursor));
        for (long sequence = cursor; sequence >= minCursor; sequence--) {
            if (sequencer.isAvailable(sequence)) {
                return sequence;
            }
        }

        throw new SafeIllegalStateException("Somehow didn't read fast enough");
    }

    @Override
    public LockWatchEvent[] getBufferSnapshot() {
        //        long cursor = ringBuffer.getCursor();
        //        Sequence sequence = new Sequence();
        //        long smallestSequence = getSmallestSequence(cursor);
        //        // The below sets the sequence value to the current cursor atomically,
        //        // So producers will not advance past this point.
        //        ringBuffer.addGatingSequences(sequence);
        //        List<LockWatchEvent> events = new ArrayList<>();
        //        try {
        //            while (smallestSequence < cursor) {
        //                if (sequencer.isAvailable(smallestSequence)) {
        //                    events.add(ringBuffer.elementAt(smallestSequence).event);
        //                }
        //                smallestSequence++;
        //            }
        //            return events.toArray(new LockWatchEvent[0]);
        //        } finally {
        //            ringBuffer.removeGatingSequence(sequence);
        //        }
        return new LockWatchEvent[0];
    }

    @Override
    public void add(LockWatchEvent.Builder doNotCaptureEventBuilderThisIsLongOnPurpose) {
        ringBuffer.publishEvent(
                (elem, sequence, builder) -> {
                    Optional.ofNullable(elem.event)
                            .flatMap(replacedEvent -> replacedEvent.accept(LockWatchEventMetadataVisitor.INSTANCE))
                            .ifPresent(this::decrementMetadataCounters);
                    elem.event = builder.build(sequence);
                    elem.event
                            .accept(LockWatchEventMetadataVisitor.INSTANCE)
                            .ifPresent(this::incrementMetadataCounters);
                },
                doNotCaptureEventBuilderThisIsLongOnPurpose);
    }

    @Override
    public Optional<NextEvents> getNextEvents(long version) {
        Preconditions.checkArgument(version >= -1, "Wrong version");
        long cursor = ringBuffer.getCursor();
        if (cursor - version > BUFFER_SIZE) {
            // Fail early and force a snapshot. There's very likely no way this consumer is up to date.
            return Optional.empty();
        }

        // This code is a "let's read what we have and figure out if what we read actually is self-consistent".
        // IT HOPEFULLY WORKS!

        // This likely over-allocates but to allocate better we'd need to figure out the latest available slot.
        // This is likely an overkill.
        List<LockWatchEvent> lockWatchEvents = new ArrayList<>(Math.max(0, Ints.checkedCast(cursor - version)));
        for (long curVersion = version + 1; curVersion <= cursor; curVersion++) {
            if (sequencer.isAvailable(curVersion)) {
                LockWatchEvent lockWatchEvent = ringBuffer.elementAt(curVersion).event;
                if (lockWatchEvent.sequence() != curVersion) {
                    log.info("We could not compute the next events, producers are too fast.");
                    // Something went wrong, we should snapshot.
                    return Optional.empty();
                }
                lockWatchEvents.add(lockWatchEvent);
            } else {
                break;
            }
        }

        if (version != -1 && !isEventInBuffer(version)) {
            return Optional.empty();
        }

        // Whatever we captured in lockWatchEvents, should actually be self-consistent.
        // If we captured no events, we're up-to-date, so return the last version.
        // This should also correctly calculate the version in case of current version -1 and there are no events.
        long newVersion = lockWatchEvents.stream()
                .mapToLong(LockWatchEvent::sequence)
                .max()
                .orElse(version);
        return Optional.of(new NextEvents(lockWatchEvents, newVersion));
    }

    @Override
    public int capacity() {
        return BUFFER_SIZE;
    }

    private long getSmallestSequence(long cursor) {
        return Math.max(0, cursor - BUFFER_SIZE);
    }

    private boolean isEventInBuffer(long version) {
        if (sequencer.isAvailable(version)) {
            LockWatchEvent lockWatchEvent = ringBuffer.elementAt(version).event;
            return lockWatchEvent.sequence() == version;
        }
        return false;
    }

    private static final class RingBufferElement {
        private LockWatchEvent event;
    }

    private void incrementMetadataCounters(LockRequestMetadata metadata) {
        changeMetadataCounter.inc(metadata.lockDescriptorToChangeMetadata().size());
        eventsWithMetadataCounter.inc();
    }

    private void decrementMetadataCounters(LockRequestMetadata metadata) {
        changeMetadataCounter.dec(metadata.lockDescriptorToChangeMetadata().size());
        eventsWithMetadataCounter.dec();
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
