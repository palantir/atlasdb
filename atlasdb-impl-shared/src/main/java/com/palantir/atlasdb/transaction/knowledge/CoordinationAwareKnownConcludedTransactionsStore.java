/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.google.common.collect.Range;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CoordinationAwareKnownConcludedTransactionsStore {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationAwareKnownConcludedTransactionsStore.class);
    // todo(Snanda): to be wired in with `TransactionSchemaManager`
    private final Function<Long, TimestampPartitioningMap<Integer>> internalSchemaSnapshotGetter;
    private final KnownConcludedTransactionsStore delegate;

    public CoordinationAwareKnownConcludedTransactionsStore(
            Function<Long, TimestampPartitioningMap<Integer>> internalSchemaSnapshotGetter,
            KnownConcludedTransactionsStore delegate) {
        this.internalSchemaSnapshotGetter = internalSchemaSnapshotGetter;
        this.delegate = delegate;
    }

    public Optional<TimestampRangeSet> get() {
        return delegate.get();
    }

    public void supplement(Range<Long> closedTimestampRangeToAdd) {
        long lastSweptTimestamp = closedTimestampRangeToAdd.upperEndpoint();

        Map<Range<Long>, Integer> timestampRanges = internalSchemaSnapshotGetter
                .apply(lastSweptTimestamp)
                .rangeMapView()
                .asMapOfRanges();

        Set<Range<Long>> rangesOnTransaction4 = KeyedStream.stream(timestampRanges)
                .filter(schemaVersion -> schemaVersion.equals(TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION))
                .keys()
                .collect(Collectors.toSet());

        Set<Range<Long>> rangesToSupplement = rangesOnTransaction4.stream()
                .map(closedTimestampRangeToAdd::intersection)
                .collect(Collectors.toSet());

        log.info(
                "Attempting to supplement the set of known concluded timestamps",
                SafeArg.of("ranges", rangesToSupplement));

        delegate.supplement(rangesToSupplement);
    }
}
