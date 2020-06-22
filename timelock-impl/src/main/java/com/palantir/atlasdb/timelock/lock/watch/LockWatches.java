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


import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchReferences;
import java.util.HashSet;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
@SuppressWarnings("UnstableApiUsage")
interface LockWatches {
    @Value.Parameter
    Set<LockWatchReferences.LockWatchReference> references();
    @Value.Parameter
    RangeSet<LockDescriptor> ranges();

    static LockWatches create() {
        return ImmutableLockWatches.of(new HashSet<>(), TreeRangeSet.create());
    }

    static LockWatches merge(LockWatches first, LockWatches second) {
        Set<LockWatchReferences.LockWatchReference> references = new HashSet<>(first.references());
        references.addAll(second.references());

        RangeSet<LockDescriptor> ranges = TreeRangeSet.create(first.ranges());
        ranges.addAll(second.ranges());

        return ImmutableLockWatches.of(references, ranges);
    }
}
