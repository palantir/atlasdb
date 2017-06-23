/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock;

import java.util.Optional;
import java.util.SortedSet;

import com.google.common.collect.Sets;

public class ImmutableTimestampTracker {

    private final SortedSet<Long> timestamps = Sets.newTreeSet();

    public synchronized void add(long timestamp) {
        timestamps.add(timestamp);
    }

    public synchronized void remove(long timestamp) {
        timestamps.remove(timestamp);
    }

    public synchronized Optional<Long> getImmutableTimestamp() {
        if (timestamps.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(timestamps.first());
    }

    public AsyncLock getLockFor(long timestamp) {
        return new ImmutableTimestampLock(timestamp, this);
    }

}
