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

package com.palantir.atlasdb.transaction.knowledge.coordinated;

import com.google.common.collect.Range;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactionsImpl;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CoordinationAwareKnownConcludedTransactionsStore implements KnownConcludedTransactions {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationAwareKnownConcludedTransactionsStore.class);

    // todo(Snanda): to be wired in with `TransactionSchemaManager`
    private final Function<Long, TimestampPartitioningMap<Integer>> internalSchemaSnapshotGetter;
    private final KnownConcludedTransactionsImpl delegate;

    public CoordinationAwareKnownConcludedTransactionsStore(
            Function<Long, TimestampPartitioningMap<Integer>> internalSchemaSnapshotGetter,
            KnownConcludedTransactionsImpl delegate) {
        this.internalSchemaSnapshotGetter = internalSchemaSnapshotGetter;
        this.delegate = delegate;
    }

    @Override
    public boolean isKnownConcluded(long startTimestamp, Consistency consistency) {
        return delegate.isKnownConcluded(startTimestamp, consistency);
    }

    @Override
    public void addConcludedTimestamps(Range<Long> closedTimestampRangeToAdd) {
        Map<Range<Long>, Integer> timestampRanges =
                latestTimestampRangesSnapshot(closedTimestampRangeToAdd.upperEndpoint());
        verifyTimestampRangeSchema(timestampRanges);

        Set<Range<Long>> rangesToSupplement = getRangesToSupplement(closedTimestampRangeToAdd, timestampRanges);

        if (!rangesToSupplement.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Attempting to supplement the set of known concluded timestamps",
                        SafeArg.of("ranges", rangesToSupplement));
            }

            delegate.addConcludedTimestamps(rangesToSupplement);
        }
    }

    @Override
    public void addConcludedTimestamps(Set<Range<Long>> knownConcludedIntervals) {
        knownConcludedIntervals.forEach(this::addConcludedTimestamps);
    }

    @Override
    public long lastLocallyKnownConcludedTimestamp() {
        return delegate.lastLocallyKnownConcludedTimestamp();
    }

    private static Set<Range<Long>> getRangesToSupplement(
            Range<Long> closedTsRangeToConclude, Map<Range<Long>, Integer> timestampRanges) {
        return KeyedStream.stream(timestampRanges)
                .filter(schemaVersion -> schemaVersion >= TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .filterKeys(closedTsRangeToConclude::isConnected)
                .mapKeys(closedTsRangeToConclude::intersection)
                .keys()
                .collect(Collectors.toSet());
    }

    private static void verifyTimestampRangeSchema(Map<Range<Long>, Integer> timestampRanges) {
        Set<Integer> unknownSchemas = timestampRanges.values().stream()
                .filter(schemaVersion -> schemaVersion > TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .collect(Collectors.toSet());
        if (!unknownSchemas.isEmpty()) {
            log.error(
                    "Found an unknown schema version. Will block further progress of TTS to avoid"
                            + " completeness issues.",
                    SafeArg.of("unknownSchemas", unknownSchemas));
        }
    }

    private Map<Range<Long>, Integer> latestTimestampRangesSnapshot(long lastSweptTimestamp) {
        return internalSchemaSnapshotGetter
                .apply(lastSweptTimestamp)
                .rangeMapView()
                .asMapOfRanges();
    }
}
