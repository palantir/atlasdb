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

package com.palantir.atlasdb;

import com.palantir.atlasdb.cache.CommitStateCache;
import com.palantir.atlasdb.cache.DefaultCommitStateCache;
import com.palantir.atlasdb.cache.OffHeapCommitStateCache;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public final class ComparingCommitStateCache implements CommitStateCache<Long> {
    private final CommitStateCache<Long> first;
    private final CommitStateCache<Long> second;

    public static CommitStateCache comparingOffHeapForTests(
            MetricsManager metricRegistry, PersistentStore persistentStore) {
        CommitStateCache first = new DefaultCommitStateCache(
                metricRegistry.getRegistry(), () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);

        CommitStateCache second = OffHeapCommitStateCache.create(
                persistentStore,
                metricRegistry.getTaggedRegistry(),
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
        return new ComparingCommitStateCache(first, second);
    }

    private ComparingCommitStateCache(CommitStateCache first, CommitStateCache second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public synchronized void clear() {
        first.clear();
        second.clear();
    }

    @Override
    public synchronized void putAlreadyCommittedTransaction(Long startTimestamp, Long commit) {
        first.putAlreadyCommittedTransaction(startTimestamp, commit);
        second.putAlreadyCommittedTransaction(startTimestamp, commit);
    }

    @Nullable
    @Override
    public synchronized Long getCommitStateIfPresent(Long startTimestamp) {
        Long firstCommitTimestamp = first.getCommitStateIfPresent(startTimestamp);
        Long secondCommitTimestamp = second.getCommitStateIfPresent(startTimestamp);
        if (firstCommitTimestamp == null || secondCommitTimestamp == null) {
            return Stream.of(firstCommitTimestamp, secondCommitTimestamp)
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }
        Preconditions.checkState(
                firstCommitTimestamp.equals(secondCommitTimestamp),
                "There is a bug in cache implementation",
                SafeArg.of("startTimestamp", startTimestamp),
                SafeArg.of("firstCommitTimestamp", firstCommitTimestamp),
                SafeArg.of("secondCommitTimestamp", secondCommitTimestamp));
        return firstCommitTimestamp;
    }
}
