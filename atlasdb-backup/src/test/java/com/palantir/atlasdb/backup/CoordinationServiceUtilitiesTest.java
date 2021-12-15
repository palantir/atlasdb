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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class CoordinationServiceUtilitiesTest {
    @Test
    public void correctlyBoundsTimestampsForRestore() {
        long lowerBoundForRestore = 5L;
        long maxTimestampForTransactions1 = 10L;
        long upperBoundForRestore = 15L;

        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closed(1L, maxTimestampForTransactions1), 1)
                .put(Range.greaterThan(maxTimestampForTransactions1), 2)
                .build();
        TimestampPartitioningMap<Integer> timestampsMap = TimestampPartitioningMap.of(rangeMap);
        InternalSchemaMetadata metadata = InternalSchemaMetadata.builder()
                .timestampToTransactionsTableSchemaVersion(timestampsMap)
                .build();
        ValueAndBound<InternalSchemaMetadata> value = ValueAndBound.of(metadata, 1000L);
        InternalSchemaMetadataState state = InternalSchemaMetadataState.of(value);

        Map<FullyBoundedTimestampRange, Integer> boundedMap = CoordinationServiceUtilities.getCoordinationMapOnRestore(
                Optional.of(state), upperBoundForRestore, lowerBoundForRestore);

        Map<FullyBoundedTimestampRange, Integer> expected = ImmutableMap.of(
                FullyBoundedTimestampRange.of(Range.closed(lowerBoundForRestore, maxTimestampForTransactions1)), 1,
                FullyBoundedTimestampRange.of(Range.openClosed(maxTimestampForTransactions1, upperBoundForRestore)), 2);

        assertThat(boundedMap).hasSize(2);
        assertThat(boundedMap).containsExactlyInAnyOrderEntriesOf(expected);
    }
}
