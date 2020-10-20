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
package com.palantir.atlasdb.timelock.lock;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.palantir.lock.LockDescriptor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class LockCollection {

    private final LoadingCache<LockDescriptor, AsyncLock> locksById;

    public LockCollection() {
        locksById = Caffeine.newBuilder().weakValues().build(ExclusiveLock::new);
    }

    public OrderedLocks getAll(Set<LockDescriptor> descriptors) {
        List<LockDescriptor> orderedDescriptors = sort(descriptors);

        List<AsyncLock> locks = Lists.newArrayListWithExpectedSize(descriptors.size());
        for (LockDescriptor descriptor : orderedDescriptors) {
            locks.add(getLock(descriptor));
        }

        return OrderedLocks.fromOrderedList(locks);
    }

    private static List<LockDescriptor> sort(Set<LockDescriptor> descriptors) {
        List<LockDescriptor> orderedDescriptors = new ArrayList<>(descriptors);
        orderedDescriptors.sort(Comparator.naturalOrder());
        return orderedDescriptors;
    }

    private AsyncLock getLock(LockDescriptor descriptor) {
        return locksById.get(descriptor);
    }
}
