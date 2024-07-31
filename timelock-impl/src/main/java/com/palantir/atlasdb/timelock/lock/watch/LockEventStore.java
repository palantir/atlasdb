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

package com.palantir.atlasdb.timelock.lock.watch;

import com.palantir.lock.watch.LockWatchEvent;
import java.util.Optional;

public interface LockEventStore {
    /**
     * Should return {@code -1} if there are no events.
     */
    long lastVersion();

    // This method is only for one-off diagnostics purposes.
    LockWatchEvent[] getBufferSnapshot();

    void add(LockWatchEvent.Builder eventBuilder);

    /**
     * Returns next events (list of events can be empty if client up to date),
     * or empty optional if client too far behind.
     */
    Optional<NextEvents> getNextEvents(long version);

    int capacity();
}
