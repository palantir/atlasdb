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

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.palantir.lock.watch.LockWatchEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ArrayLockEventSlidingWindow {
    private final LockWatchEvent[] buffer;
    private final int maxSize;
    private long nextSequence = 0;

    ArrayLockEventSlidingWindow(int maxSize) {
        this.buffer = new LockWatchEvent[maxSize];
        this.maxSize = maxSize;
    }

    long lastVersion() {
        return nextSequence - 1;
    }

    void add(LockWatchEvent.Builder eventBuilder) {
        LockWatchEvent event = eventBuilder.build(nextSequence);
        buffer[LongMath.mod(nextSequence, maxSize)] = event;
        nextSequence++;
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
}
