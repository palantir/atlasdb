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

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions1TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions2TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public final class CoordinationServiceUtilitiesTest {
    private static final DefaultRetryPolicy POLICY = DefaultRetryPolicy.INSTANCE;

    private static final long IMMUTABLE_TIMESTAMP = 100L;
    private static final long FASTFORWARD_TIMESTAMP = 400L;

    // During restores, we need to clean transactions tables in between two timestamps.
    // It's possible that, in between _those_ timestamps, AtlasDB changed which transactions
    // table to use. This is recorded in InternalSchemaMetadata.
    //
    // In this test, we simulate a switch from transactions1 to transactions2 at timestamp 10
    // We should return the [5-10] range for transactions1, and the (10-15] range for transactions2.
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

    @Test
    public void noCoordServiceWithImmutableCreatesRangeImmutableToFfWithSchema1() {
        List<TransactionsTableInteraction> txnInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        CoordinationServiceUtilities.getCoordinationMapOnRestore(
                                Optional.empty(), FASTFORWARD_TIMESTAMP, IMMUTABLE_TIMESTAMP),
                        POLICY);

        assertThat(txnInteractions).hasSize(1);
        assertThat(txnInteractions.get(0))
                .isInstanceOf(Transactions1TableInteraction.class)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closed(IMMUTABLE_TIMESTAMP, FASTFORWARD_TIMESTAMP)));
    }

    @Test
    public void immutablePresentThenStartAtImmutable() {
        Map<Range<Long>, Integer> rangesWithSchemas = ImmutableMap.of(Range.atLeast(AtlasDbConstants.STARTING_TS), 1);
        final long coordServiceBound = FASTFORWARD_TIMESTAMP + 100L;
        Optional<InternalSchemaMetadataState> coordService = createCoordService(rangesWithSchemas, coordServiceBound);

        List<TransactionsTableInteraction> txnInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        CoordinationServiceUtilities.getCoordinationMapOnRestore(
                                coordService, FASTFORWARD_TIMESTAMP, IMMUTABLE_TIMESTAMP),
                        POLICY);

        assertThat(txnInteractions).hasSize(1);
        assertThat(txnInteractions.get(0))
                .isInstanceOf(Transactions1TableInteraction.class)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closed(IMMUTABLE_TIMESTAMP, FASTFORWARD_TIMESTAMP)));
    }

    @Test
    public void coordServiceBoundLessThanFfStopsAtBound() {
        Map<Range<Long>, Integer> rangesWithSchemas = ImmutableMap.of(Range.atLeast(AtlasDbConstants.STARTING_TS), 1);
        final long coordServiceBound = FASTFORWARD_TIMESTAMP - 10L;
        Optional<InternalSchemaMetadataState> coordService = createCoordService(rangesWithSchemas, coordServiceBound);

        List<TransactionsTableInteraction> txnInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        CoordinationServiceUtilities.getCoordinationMapOnRestore(
                                coordService, FASTFORWARD_TIMESTAMP, IMMUTABLE_TIMESTAMP),
                        POLICY);

        assertThat(txnInteractions).hasSize(1);
        assertThat(txnInteractions.get(0))
                .isInstanceOf(Transactions1TableInteraction.class)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closed(IMMUTABLE_TIMESTAMP, coordServiceBound)));
    }

    @Test
    public void coordServiceBoundGreaterThanFfStopsAtFf() {
        Map<Range<Long>, Integer> rangesWithSchemas = ImmutableMap.of(Range.atLeast(AtlasDbConstants.STARTING_TS), 1);
        final long coordServiceBound = FASTFORWARD_TIMESTAMP + 1000L;
        Optional<InternalSchemaMetadataState> coordService = createCoordService(rangesWithSchemas, coordServiceBound);

        List<TransactionsTableInteraction> txnInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        CoordinationServiceUtilities.getCoordinationMapOnRestore(
                                coordService, FASTFORWARD_TIMESTAMP, IMMUTABLE_TIMESTAMP),
                        POLICY);

        assertThat(txnInteractions).hasSize(1);
        assertThat(txnInteractions.get(0))
                .isInstanceOf(Transactions1TableInteraction.class)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closed(IMMUTABLE_TIMESTAMP, FASTFORWARD_TIMESTAMP)));
    }

    @Test
    public void cleanMultipleRangesWithSchemasWithinBounds() {
        final long migrateToTxns2 = IMMUTABLE_TIMESTAMP + FASTFORWARD_TIMESTAMP / 2;
        Map<Range<Long>, Integer> rangesWithSchemas = ImmutableMap.of(
                Range.closedOpen(AtlasDbConstants.STARTING_TS, migrateToTxns2), 1,
                Range.atLeast(migrateToTxns2), 2);
        final long coordServiceBound = FASTFORWARD_TIMESTAMP + 1000L;
        Optional<InternalSchemaMetadataState> coordService = createCoordService(rangesWithSchemas, coordServiceBound);

        List<TransactionsTableInteraction> txnInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        CoordinationServiceUtilities.getCoordinationMapOnRestore(
                                coordService, FASTFORWARD_TIMESTAMP, IMMUTABLE_TIMESTAMP),
                        POLICY);

        assertThat(txnInteractions).hasSize(2);
        TransactionsTableInteraction txn1 = txnInteractions.stream()
                .filter(txn -> txn instanceof Transactions1TableInteraction)
                .findFirst()
                .get();
        TransactionsTableInteraction txn2 = txnInteractions.stream()
                .filter(txn -> txn instanceof Transactions2TableInteraction)
                .findFirst()
                .get();
        assertThat(txn1)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closedOpen(IMMUTABLE_TIMESTAMP, migrateToTxns2)));
        assertThat(txn2)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closed(migrateToTxns2, FASTFORWARD_TIMESTAMP)));
    }

    @Test
    public void skipRangesWithSchemasOutsideOfBounds() {
        final long migrateToTxns2 = AtlasDbConstants.STARTING_TS + IMMUTABLE_TIMESTAMP / 2;
        Map<Range<Long>, Integer> rangesWithSchemas = ImmutableMap.of(
                Range.closedOpen(AtlasDbConstants.STARTING_TS, migrateToTxns2), 1,
                Range.atLeast(migrateToTxns2), 2);
        final long coordServiceBound = FASTFORWARD_TIMESTAMP + 1000L;
        Optional<InternalSchemaMetadataState> coordService = createCoordService(rangesWithSchemas, coordServiceBound);

        List<TransactionsTableInteraction> txnInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        CoordinationServiceUtilities.getCoordinationMapOnRestore(
                                coordService, FASTFORWARD_TIMESTAMP, IMMUTABLE_TIMESTAMP),
                        POLICY);

        assertThat(txnInteractions).hasSize(1);
        assertThat(txnInteractions.get(0))
                .isInstanceOf(Transactions2TableInteraction.class)
                .extracting(TransactionsTableInteraction::getTimestampRange)
                .isEqualTo(FullyBoundedTimestampRange.of(Range.closed(IMMUTABLE_TIMESTAMP, FASTFORWARD_TIMESTAMP)));
    }

    private static Optional<InternalSchemaMetadataState> createCoordService(
            Map<Range<Long>, Integer> rangesWithSchemas, long bound) {
        ImmutableRangeMap.Builder<Long, Integer> rangeMapBuilder = ImmutableRangeMap.builder();
        rangesWithSchemas.forEach(rangeMapBuilder::put);
        RangeMap<Long, Integer> rangeMap = rangeMapBuilder.build();

        InternalSchemaMetadata internalSchemaMetadata = InternalSchemaMetadata.builder()
                .timestampToTransactionsTableSchemaVersion(TimestampPartitioningMap.of(rangeMap))
                .build();

        return Optional.of(
                InternalSchemaMetadataState.of(Optional.of(ValueAndBound.of(internalSchemaMetadata, bound))));
    }
}
