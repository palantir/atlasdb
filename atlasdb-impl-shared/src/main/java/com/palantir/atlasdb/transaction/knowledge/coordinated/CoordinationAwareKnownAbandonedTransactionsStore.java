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

import com.google.common.collect.Iterables;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.KnownAbandonedTransactions;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CoordinationAwareKnownAbandonedTransactionsStore {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationAwareKnownConcludedTransactionsStore.class);
    private final Function<Long, TimestampPartitioningMap<Integer>> internalSchemaSnapshotGetter;
    private final KnownAbandonedTransactions delegate;

    public CoordinationAwareKnownAbandonedTransactionsStore(
            Function<Long, TimestampPartitioningMap<Integer>> internalSchemaSnapshotGetter,
            KnownAbandonedTransactions delegate) {
        this.internalSchemaSnapshotGetter = internalSchemaSnapshotGetter;
        this.delegate = delegate;
    }

    public void addAbandonedTimestamps(Set<Long> abandonedTimestamps) {
        List<Long> sortedTs = abandonedTimestamps.stream().sorted().collect(Collectors.toList());
        Long greatestAbandonedTs = Iterables.getLast(sortedTs);
        RangeMap<Long, Integer> transactionsSchemaMap = latestTimestampRangesSnapshot(greatestAbandonedTs);
        Set<Long> abandonedTsOnSchema4 = sortedTs.stream()
                .filter(ts -> {
                    Optional<Integer> maybeSchema = Optional.ofNullable(transactionsSchemaMap.get(ts));
                    return maybeSchema
                            .map(integer -> integer.equals(TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION))
                            .orElse(false);
                })
                .collect(Collectors.toSet());
        delegate.addAbandonedTimestamps(abandonedTsOnSchema4);
    }

    private RangeMap<Long, Integer> latestTimestampRangesSnapshot(long lastSweptTimestamp) {
        return internalSchemaSnapshotGetter.apply(lastSweptTimestamp).rangeMapView();
    }
}
