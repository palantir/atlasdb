/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timestamp;

import com.google.common.annotations.VisibleForTesting;

public class PersistentUpperLimit {

    /**
     * Some internal atlas clients have behavior tied to this value and will need to
     * be updated if the value changes.
     */
    @VisibleForTesting
    static final long BUFFER = 1_000_000;

    private volatile long currentLimit;
    private final TimestampBoundStore store;

    public PersistentUpperLimit(TimestampBoundStore boundStore) {
        this.store = boundStore;
        this.currentLimit = boundStore.getUpperLimit();
    }

    public long get() {
        return currentLimit;
    }

    public void increaseToAtLeast(long newLimit) {
        if (newLimit > currentLimit) {
            updateLimit(newLimit);
        }
    }

    private synchronized void updateLimit(long newLimit) {
        if (currentLimit >= newLimit) {
            return;
        }

        long newLimitWithBuffer = Math.addExact(newLimit, BUFFER);
        storeUpperLimit(newLimitWithBuffer);
        currentLimit = newLimitWithBuffer;
    }

    private void storeUpperLimit(long upperLimit) {
        DebugLogger.willStoreNewUpperLimit(upperLimit);
        store.storeUpperLimit(upperLimit);
        DebugLogger.didStoreNewUpperLimit(upperLimit);
    }
}
