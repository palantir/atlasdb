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

import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cache.OffHeapTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public final class ComparingTimestampCache implements TimestampCache {
    private final TimestampCache first;
    private final TimestampCache second;

    public static TimestampCache comparingOffHeapForTests(
            MetricsManager metricRegistry, PersistentStore persistentStore) {
        TimestampCache first = new DefaultTimestampCache(
                metricRegistry.getRegistry(), () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);

        TimestampCache second = OffHeapTimestampCache.create(
                persistentStore,
                metricRegistry.getTaggedRegistry(),
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
        return new ComparingTimestampCache(first, second);
    }

    private ComparingTimestampCache(TimestampCache first, TimestampCache second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public synchronized void clear() {
        first.clear();
        second.clear();
    }

    @Override
    public synchronized void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        first.putAlreadyCommittedTransaction(startTimestamp, commitTimestamp);
        second.putAlreadyCommittedTransaction(startTimestamp, commitTimestamp);
    }

    @Nullable
    @Override
    public synchronized Long getCommitTimestampIfPresent(Long startTimestamp) {
        Long firstCommitTimestamp = first.getCommitTimestampIfPresent(startTimestamp);
        Long secondCommitTimestamp = second.getCommitTimestampIfPresent(startTimestamp);
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
