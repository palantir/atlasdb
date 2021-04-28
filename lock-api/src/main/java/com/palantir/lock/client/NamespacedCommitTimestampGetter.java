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

package com.palantir.lock.client;

import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.cache.ValueCacheUpdater;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchEventCache;

public class NamespacedCommitTimestampGetter implements CommitTimestampGetter {
    private final Namespace namespace;
    private final LockWatchEventCache eventCache;
    private final ValueCacheUpdater valueCache;
    private final ReferenceTrackingWrapper<MultiClientCommitTimestampGetter> referenceTrackingBatcher;

    public NamespacedCommitTimestampGetter(
            LockWatchEventCache eventCache,
            Namespace namespace,
            ValueCacheUpdater valueCache,
            ReferenceTrackingWrapper<MultiClientCommitTimestampGetter> referenceTrackingBatcher) {
        this.namespace = namespace;
        this.eventCache = eventCache;
        this.valueCache = valueCache;
        this.referenceTrackingBatcher = referenceTrackingBatcher;
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return referenceTrackingBatcher
                .getDelegate()
                .getCommitTimestamp(namespace, startTs, commitLocksToken, eventCache, valueCache);
    }

    @Override
    public void close() {
        referenceTrackingBatcher.close();
    }
}
