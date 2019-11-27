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

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.palantir.lock.watch.LockWatchEvent;

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
     */
    public synchronized void add(LockWatchEvent.Builder eventBuilder) {
        LockWatchEvent event = eventBuilder.build(nextSequence);
        buffer[LongMath.mod(nextSequence, maxSize)] = event;
        nextSequence++;
    }

    /**
     * Returns a list of all events that occurred starting with requested version up to the most recent version, ordered
     * by consecutively increasing sequence numbers. If the list cannot be created, either a priori or because new
     * events are added to the window during execution of this method causing eviction an event before it was read, the
     * method will return a singleton list containing only the most recent event, if it exists.
     */
    public List<LockWatchEvent> getFromVersion(long version) {
        long lastWrittenSequence = nextSequence - 1;

        if (versionInTheFuture(version, lastWrittenSequence) || versionTooOld(version, lastWrittenSequence)) {
            return ImmutableList.of();
        }

        int startIndex = LongMath.mod(version, maxSize);
        int windowSize = Ints.saturatedCast(lastWrittenSequence - version + 1);
        List<LockWatchEvent> events = new ArrayList<>(windowSize);

        for (int i = startIndex; events.size() < windowSize; i = (i + 1) % maxSize) {
            events.add(buffer[i]);
        }

        return validateConsistencyOrReturnEmpty(version, events);
    }

    private boolean versionInTheFuture(long lastVersion, long lastWrittenSequence) {
        return lastVersion > lastWrittenSequence;
    }

    private boolean versionTooOld(long lastVersion, long lastWrittenSequence) {
        return lastWrittenSequence - lastVersion + 1 > maxSize;
    }

    private List<LockWatchEvent> validateConsistencyOrReturnEmpty(long version, List<LockWatchEvent> events) {
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).sequence() != i + version) {
                return ImmutableList.of();
            }
        }
        return events;
    }
}
