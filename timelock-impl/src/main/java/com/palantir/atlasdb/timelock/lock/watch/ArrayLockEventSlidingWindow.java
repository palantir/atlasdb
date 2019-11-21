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
import java.util.OptionalLong;
import java.util.function.Function;

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

    public synchronized void add(Function<Long, LockWatchEvent> eventBuilder) {
        LockWatchEvent event = eventBuilder.apply(nextSequence);
        buffer[LongMath.mod(nextSequence, maxSize)] = event;
        nextSequence++;
    }

    public List<LockWatchEvent> getFromVersion(OptionalLong version) {
        if (!version.isPresent()) {
            return getLastEntry();
        }
        long lastWrittenSequence = nextSequence - 1;
        long lastVersion = version.getAsLong();

        if (lastVersion > lastWrittenSequence || lastWrittenSequence - lastVersion + 1 > maxSize) {
            return getLastEntry();
        }

        int start = LongMath.mod(lastVersion, maxSize);

        int numEvents = Math.min(maxSize, Ints.saturatedCast(lastWrittenSequence - lastVersion + 1));
        List<LockWatchEvent> events = new ArrayList<>(numEvents);

        for (int i = start; events.size() < numEvents; i = (i + 1) % maxSize) {
            events.add(buffer[i]);
        }

        return validateConsistencyOrReturnEmpty(lastVersion, events);
    }

    private ImmutableList<LockWatchEvent> getLastEntry() {
        return nextSequence > 0
                ? ImmutableList.of(buffer[LongMath.mod(nextSequence - 1, maxSize)])
                : ImmutableList.of();
    }

    private List<LockWatchEvent> validateConsistencyOrReturnEmpty(long version, List<LockWatchEvent> events) {
        for (int i = 0; i < events.size(); i++, version++) {
            if (events.get(i).sequence() != version) {
                return getLastEntry();
            }
        }
        return events;
    }
}
