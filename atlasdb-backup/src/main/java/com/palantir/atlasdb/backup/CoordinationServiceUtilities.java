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

package com.palantir.atlasdb.backup;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.backup.transaction.FullyBoundedTimestampRange;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("UnstableApiUsage")
public final class CoordinationServiceUtilities {
    // before the coordination service all transactions were schema 1, i.e. in _transactions
    private static final ImmutableRangeMap<Long, Integer> PRE_COORDINATION_TRANSACTIONS_MAP =
            ImmutableRangeMap.of(Range.atLeast(AtlasDbConstants.STARTING_TS), 1);

    private CoordinationServiceUtilities() {}

    public static Map<FullyBoundedTimestampRange, Integer> getCoordinationMapOnRestore(
            Optional<InternalSchemaMetadataState> schemaMetadataState, long fastForwardTs, long immutableTs) {
        RangeMap<Long, Integer> unbounded = convertInternalSchemaMetadataToRangeMap(schemaMetadataState);

        RangeMap<Long, Integer> fastForwardBounded = addInclusiveUpperBoundToRangeMap(unbounded, fastForwardTs);

        RangeMap<Long, Integer> bounded = addInclusiveLowerBoundToRangeMap(fastForwardBounded, immutableTs);

        return KeyedStream.stream(bounded.asMapOfRanges())
                .mapKeys(FullyBoundedTimestampRange::of)
                .collectToMap();
    }

    private static RangeMap<Long, Integer> convertInternalSchemaMetadataToRangeMap(
            Optional<InternalSchemaMetadataState> internalSchemaMetadata) {
        return internalSchemaMetadata
                .flatMap(InternalSchemaMetadataState::value)
                .flatMap(CoordinationServiceUtilities::convertValueAndBoundToRangeMap)
                .orElse(PRE_COORDINATION_TRANSACTIONS_MAP);
    }

    private static Optional<RangeMap<Long, Integer>> convertValueAndBoundToRangeMap(
            ValueAndBound<InternalSchemaMetadata> valueAndBound) {
        return valueAndBound
                .value()
                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                .map(TimestampPartitioningMap::rangeMapView)
                .map(rangeMap -> addInclusiveUpperBoundToRangeMap(rangeMap, valueAndBound.bound()));
    }

    private static RangeMap<Long, Integer> addInclusiveUpperBoundToRangeMap(
            RangeMap<Long, Integer> rangeMap, long upperBound) {
        return rangeMap.subRangeMap(Range.atMost(upperBound));
    }

    private static RangeMap<Long, Integer> addInclusiveLowerBoundToRangeMap(
            RangeMap<Long, Integer> rangeMap, long lowerBound) {
        return rangeMap.subRangeMap(Range.atLeast(lowerBound));
    }
}
