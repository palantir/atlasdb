/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.lock.watch.LockWatchEvent;

public interface ValueStore {
    void reset();

    void applyEvent(LockWatchEvent event);

    /**
     * Stores a value in the central cache. Note that this will throw if there is currently an existing entry with a
     * different value, or if the value is locked.
     */
    void putValue(CellReference cellReference, CacheValue value);

    ValueCacheSnapshot getSnapshot();
}
