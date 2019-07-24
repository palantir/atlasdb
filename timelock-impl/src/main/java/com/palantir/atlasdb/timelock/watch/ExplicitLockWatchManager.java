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

package com.palantir.atlasdb.timelock.watch;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.palantir.lock.LockDescriptor;

public class ExplicitLockWatchManager implements LockWatchManager {
    private final Multimap<LockDescriptor, LockWatch> explicitDescriptorsToWatches;

    public ExplicitLockWatchManager() {
        this.explicitDescriptorsToWatches = Multimaps.synchronizedListMultimap(MultimapBuilder.hashKeys()
                .arrayListValues()
                .build());
    }

    @Override
    public LockEventProcessor getEventProcessor() {
        return new LockEventProcessor() {
            @Override
            public void registerLock(LockDescriptor descriptor) {
                explicitDescriptorsToWatches.get(descriptor).forEach(LockWatch::registerLock);
            }

            @Override
            public void registerUnlock(LockDescriptor descriptor) {
                explicitDescriptorsToWatches.get(descriptor).forEach(LockWatch::registerUnlock);
            }
        };
    }

    @Override
    public void seedProcessor(LockPredicate predicate, LockWatch watch) {
        if (predicate instanceof ExplicitLockPredicate) {
            ExplicitLockPredicate explicitLockPredicate = (ExplicitLockPredicate) predicate;
            explicitLockPredicate.descriptors().forEach(descriptor ->
                    explicitDescriptorsToWatches.put(descriptor, watch));
        }
    }

    @Override
    public void unseedProcessor(LockPredicate predicate, LockWatch watch) {
        if (predicate instanceof ExplicitLockPredicate) {
            ExplicitLockPredicate explicitLockPredicate = (ExplicitLockPredicate) predicate;
            explicitLockPredicate.descriptors().forEach(descriptor ->
                    explicitDescriptorsToWatches.remove(descriptor, watch));
        }
    }
}
