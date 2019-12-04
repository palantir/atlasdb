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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.palantir.lock.watch.LockWatchEvent;

@ThreadSafe
public class ArrayLockEventSlidingWindow {
    private final LockWatchEvent[] buffer;
    private final int maxSize;
    private volatile long nextSequence = 0;

    public ArrayLockEventSlidingWindow(int maxSize) {
        this.buffer = new LockWatchEvent[maxSize];
        this.maxSize = maxSize;
    }

    public OptionalLong getVersion() {
        return nextSequence == 0 ? OptionalLong.empty() : OptionalLong.of(nextSequence - 1);
    }

    /**
     * Adds an event to the sliding window. Assigns a unique sequence to the event.
     *
     * Note on concurrency:
     * 1. Each write to buffer is followed by a write to nextSequence, which is volatile.
     */
    public synchronized void add(LockWatchEvent.Builder eventBuilder) {
        LockWatchEvent event = eventBuilder.build(nextSequence);
        buffer[LongMath.mod(nextSequence, maxSize)] = event;
        nextSequence++;
    }

    /**
     * Returns a list of all events that occurred immediately after the requested version up to the most recent version,
     * ordered by consecutively increasing sequence numbers. If the list cannot be created, either a priori or because
     * new events are added to the window during execution of this method causing eviction an event before it was read,
     * the method will return {@link Optional#empty()}.
     *
     * Note on concurrency:
     * 2. Before reading from buffer, we read nextSequence.
     *
     * 1. and 2. ensure that calls to this method have an up to date view of buffer, containing all updates made so
     * far. The buffer may be updated after the volatile read of nextSequence, and these updates may or may not be
     * visible. This does not affect correctness:
     *   a) the newer updates are not expected to be reflected in the returned list
     *   b) if (some of) the newer updates are visible and overwrite a value that should have been included in the
     *      returned list, it will may end up included in the candidate result. This will be detected by
     *      validateConsistencyOrReturnEmpty, and {@link Optional#empty()} will be returned. This correctly reflects
     *      the state where, even though all the necessary events were in the requested window at the start of executing
     *      this method, that is no longer the case when the method returns.
     */
    public Optional<List<LockWatchEvent>> getFromVersion(long version) {
        long lastWrittenSequence = nextSequence - 1;

        if (versionInTheFuture(version, lastWrittenSequence) || versionTooOld(version, lastWrittenSequence)) {
            return Optional.empty();
        }

        int startIndex = LongMath.mod(version + 1, maxSize);
        int windowSize = Ints.saturatedCast(lastWrittenSequence - version);
        List<LockWatchEvent> events = new ArrayList<>(windowSize);

        for (int i = startIndex; events.size() < windowSize; i = incrementAndMod(i)) {
            events.add(buffer[i]);
        }

        return validateConsistencyOrReturnEmpty(version, events);
    }

    private int incrementAndMod(int num) {
        num++;
        return num >= maxSize ? num % maxSize : num;
    }

    private boolean versionInTheFuture(long lastVersion, long lastWrittenSequence) {
        return lastVersion > lastWrittenSequence;
    }

    private boolean versionTooOld(long lastVersion, long lastWrittenSequence) {
        return lastWrittenSequence - lastVersion > maxSize;
    }

    private Optional<List<LockWatchEvent>> validateConsistencyOrReturnEmpty(long version, List<LockWatchEvent> events) {
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).sequence() != i + version + 1) {
                return Optional.empty();
            }
        }
        return Optional.of(events);
    }
}
