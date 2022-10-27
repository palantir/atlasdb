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

import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.AbandonedTimestampStore;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public final class CoordinationAwareKnownAbandonedTransactionsStore {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationAwareKnownAbandonedTransactionsStore.class);
    private final CoordinationService<InternalSchemaMetadata> coordinationService;
    private final AbandonedTimestampStore delegate;

    public CoordinationAwareKnownAbandonedTransactionsStore(
            CoordinationService<InternalSchemaMetadata> coordinationService, AbandonedTimestampStore delegate) {
        this.coordinationService = coordinationService;
        this.delegate = delegate;
    }

    public void addAbandonedTimestamps(Set<Long> abandonedTimestamps) {
        if (abandonedTimestamps.isEmpty()) {
            return;
        }

        RangeMap<Long, Integer> transactionsSchemaMap = latestTimestampRangesSnapshot(
                abandonedTimestamps.stream().max(Comparator.naturalOrder()).get());

        Set<Long> abandonedTsToMark = new HashSet<>();

        for (long ts : abandonedTimestamps) {
            Optional<Integer> maybeSchema = Optional.ofNullable(transactionsSchemaMap.get(ts));
            Preconditions.checkState(
                    maybeSchema.isPresent(),
                    "Found an abandoned transactions without schema version. This should never happen and likely means"
                            + " there is a bug in the transactional protocol.",
                    SafeArg.of("timestamp", ts));

            // We filter out all updates for A for transactions at lower schema
            int schema = maybeSchema.get();
            if (schema < TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION) {
                continue;
            }

            if (schema > TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION) {
                // This is log is meant to be noisy
                log.error("Updating A for unrecognized schema version.", SafeArg.of("schemaVersion", schema));
            }
            abandonedTsToMark.add(ts);
        }

        if (!abandonedTsToMark.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Attempting to mark transactions as abandoned.", SafeArg.of("count", abandonedTsToMark.size()));
            }

            delegate.markAbandoned(abandonedTsToMark);
        }
    }

    private RangeMap<Long, Integer> latestTimestampRangesSnapshot(long lastSweptTimestamp) {
        InternalSchemaMetadata internalSchemaMeta = coordinationService
                .getValueForTimestamp(lastSweptTimestamp)
                .flatMap(ValueAndBound::value)
                .orElseGet(InternalSchemaMetadata::defaultValue);
        return internalSchemaMeta.timestampToTransactionsTableSchemaVersion().rangeMapView();
    }
}
