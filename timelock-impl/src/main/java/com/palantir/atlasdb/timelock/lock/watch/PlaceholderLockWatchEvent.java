/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

public final class PlaceholderLockWatchEvent implements LockWatchEvent {
    // This is effectively a LockWatchEvent tombstone
    static final LockWatchEvent INSTANCE = new PlaceholderLockWatchEvent();

    private PlaceholderLockWatchEvent() {
        // hidden
    }

    @Override
    public long sequence() {
        return -1;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return null;
    }
}
