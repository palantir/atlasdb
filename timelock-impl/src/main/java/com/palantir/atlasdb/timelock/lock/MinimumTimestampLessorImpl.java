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

package com.palantir.atlasdb.timelock.lock;

import com.palantir.atlasdb.timelock.util.LoggableIllegalStateException;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;

final class MinimumTimestampLessorImpl implements MinimumTimestampLessor {
    @GuardedBy("this")
    private final SortedMap<Long, UUID> holdersByTimestamp = new TreeMap<>();

    @Override
    public synchronized void hold(long timestamp, UUID requestId) {
        boolean wasAdded = holdersByTimestamp.putIfAbsent(timestamp, requestId) == null;
        if (!wasAdded) {
            throw new LoggableIllegalStateException(
                    "Attempted to hold a timestamp that was already held",
                    SafeArg.of("timestamp", timestamp),
                    SafeArg.of("requestId", requestId),
                    SafeArg.of("currentHolder", holdersByTimestamp.get(timestamp)));
        }
    }

    @Override
    public synchronized void release(long timestamp, UUID requestId) {
        boolean wasRemoved = holdersByTimestamp.remove(timestamp, requestId);
        if (!wasRemoved) {
            throw new LoggableIllegalStateException(
                    "Attempted to release a timestamp that was not held or was held by another request",
                    SafeArg.of("timestamp", timestamp),
                    SafeArg.of("requestId", requestId),
                    SafeArg.of("currentHolder", holdersByTimestamp.get(timestamp)));
        }
    }

    @Override
    public synchronized Optional<Long> getMinimumLeased() {
        if (holdersByTimestamp.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(holdersByTimestamp.firstKey());
    }
}
