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

import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import java.util.Collection;
import java.util.Optional;

/**
 * Stores snapshots of the cache taken at the sequence corresponding to the provided start timestamp. De-duplicates in
 * the case of multiple start timestamps corresponding to a single sequence (which is likely as they are batched),
 * and and removes snapshots when they are no longer referenced by any live start timestamps.
 */
public interface SnapshotStore {
    void storeSnapshot(Sequence sequence, Collection<StartTimestamp> timestamps, ValueCacheSnapshot snapshot);

    void updateSnapshot(Sequence sequence, ValueCacheSnapshot snapshot);

    Optional<ValueCacheSnapshot> getSnapshot(StartTimestamp timestamp);

    Optional<ValueCacheSnapshot> getSnapshotForSequence(Sequence sequence);

    Optional<Sequence> removeTimestamp(StartTimestamp timestamp);

    void reset();
}
