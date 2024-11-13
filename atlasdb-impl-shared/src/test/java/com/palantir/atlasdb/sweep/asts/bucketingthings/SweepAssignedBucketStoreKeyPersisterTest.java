/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.schema.generated.SweepAssignedBucketsTable.SweepAssignedBucketsRow;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class SweepAssignedBucketStoreKeyPersisterTest {
    private static final ObjectPersister<TimestampRange> TIMESTAMP_RANGE_PERSISTER =
            ObjectPersister.of(TimestampRange.class, LogSafety.SAFE);

    private static final Map<Bucket, Cell> GOLDEN_BUCKETS = Map.of(
            Bucket.of(ShardAndStrategy.conservative(0), 0),
            fromBase64("95dIh9y7UDyAgAE=", "gA=="),
            Bucket.of(ShardAndStrategy.conservative(0), 1),
            fromBase64("95dIh9y7UDyAgAE=", "gQ=="),
            Bucket.of(ShardAndStrategy.conservative(1), 0),
            fromBase64("HdZZiImVreeBgAE=", "gA=="),
            Bucket.of(ShardAndStrategy.thorough(0), 10000000),
            fromBase64("S75hl43DqVyA4YagAA==", "gA=="),
            Bucket.of(ShardAndStrategy.thorough(0), 1),
            fromBase64("uOHLXMd0L9KAgAA=", "gQ=="),
            Bucket.of(ShardAndStrategy.thorough(1), 1245123123),
            fromBase64("jAxmaZeQfpWB8L39nwA=", "lw=="),
            Bucket.of(ShardAndStrategy.nonSweepable(), Long.MAX_VALUE),
            fromBase64("PpljSwhiGMuA/4FHrhR64UeuAg==", "hw=="));

    private static final Map<TimestampRange, byte[]> GOLDEN_TIMESTAMP_RANGES = Map.of(
            TimestampRange.of(912301923, Long.MAX_VALUE),
            BaseEncoding.base64().decode("OikKBfqLZW5kRXhjbHVzaXZlJQN/f39/f39/f76Nc3RhcnRJbmNsdXNpdmUkDUwJe4b7"),
            TimestampRange.openBucket(13123),
            BaseEncoding.base64().decode("OikKBfqLZW5kRXhjbHVzaXZlwY1zdGFydEluY2x1c2l2ZSQDGob7"));

    private static final Cell GOLDEN_SWEEP_BUCKET_ASSIGNER_STATE_MACHINE_CELL = Cell.create(
            BaseEncoding.base64().decode("GTH5RX8BW6N/f/8="),
            BaseEncoding.base64().decode("fw=="));

    private static final Map<ShardAndStrategy, Cell> GOLDEN_SWEEP_BUCKET_POINTER_CELLS = Map.of(
            ShardAndStrategy.conservative(0),
            fromBase64("S0/IrI4nnp+AfwE=", "fw=="),
            ShardAndStrategy.conservative(1),
            fromBase64("s1rdA0gzO/CBfwE=", "fw=="),
            ShardAndStrategy.thorough(0),
            fromBase64("SovRDVViVQKAfwA=", "fw=="),
            ShardAndStrategy.thorough(1),
            fromBase64("uPBDJgpXMfeBfwA=", "fw=="),
            ShardAndStrategy.nonSweepable(),
            fromBase64("pWhFqaJi6q2AfwI=", "fw=="));

    private static final Map<Long, Cell> GOLDEN_SWEEP_BUCKET_RECORDS_CELLS = Map.of(
            0L,
            fromBase64("odL6eJE/9Q9/gP8=", "gA=="),
            1L,
            fromBase64("odL6eJE/9Q9/gP8=", "gQ=="),
            2L,
            fromBase64("odL6eJE/9Q9/gP8=", "gg=="),
            311421L,
            fromBase64("6ty/CriS/L1/zCr/", "lQ=="),
            Long.MAX_VALUE,
            fromBase64("C4tS7h1RgGV//4FHrhR64Ueu/w==", "hw=="));

    @Test
    public void differentBucketsMapToDifferentSweepBucketsCells() {
        List<Cell> cells = buckets().stream()
                .map(SweepAssignedBucketStoreKeyPersister.INSTANCE::sweepBucketsCell)
                .collect(Collectors.toList());

        assertThat(cells).doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @MethodSource("goldenSweepBucketsCells")
    public void bucketMapsToHistoricSweepBucketsCell(Bucket bucket, Cell expectedCell) {
        Cell actualCell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsCell(bucket);
        assertThat(actualCell).isEqualTo(expectedCell);
    }

    // The tests for checking buckets map to historic rows are omitted as this is covered by the cell tests above.
    @ParameterizedTest
    @MethodSource("buckets")
    public void nextSweepBucketsRowGetsNextRowByMajorBucketIdentifier(Bucket bucket) {
        SweepAssignedBucketsRow currentRow = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsRow(bucket);
        SweepAssignedBucketsRow nextRow = SweepAssignedBucketStoreKeyPersister.INSTANCE.nextSweepBucketsRow(bucket);

        SweepAssignedBucketsRow expectedNextRow = SweepAssignedBucketsRow.of(
                currentRow.getShard(), currentRow.getMajorBucketIdentifier() + 1, currentRow.getStrategy());
        assertThat(nextRow).isEqualTo(expectedNextRow);
    }

    @ParameterizedTest
    @MethodSource("sweepableBuckets")
    public void canDeserializeCellsAndValuesBackToSweepableBucket(SweepableBucket sweepableBucket) {
        byte[] value = TIMESTAMP_RANGE_PERSISTER.trySerialize(sweepableBucket.timestampRange());
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsCell(sweepableBucket.bucket());
        SweepableBucket deserialisedSweepableBucket =
                SweepAssignedBucketStoreKeyPersister.INSTANCE.fromSweepBucketCellAndValue(
                        cell, value, TIMESTAMP_RANGE_PERSISTER);

        assertThat(deserialisedSweepableBucket).isEqualTo(sweepableBucket);
    }

    @ParameterizedTest
    @MethodSource("goldenSweepBucketCellsAndValues")
    public void canDeserializeHistoricCellsAndValuesBackToSweepableBucket(
            SweepableBucket sweepableBucket, Cell cell, byte[] value) {
        SweepableBucket deserialisedSweepableBucket =
                SweepAssignedBucketStoreKeyPersister.INSTANCE.fromSweepBucketCellAndValue(
                        cell, value, TIMESTAMP_RANGE_PERSISTER);

        assertThat(deserialisedSweepableBucket).isEqualTo(sweepableBucket);
    }

    @Test
    public void sweepBucketAssignerStateMachineCellMatchesHistoricCell() {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        assertThat(cell).isEqualTo(GOLDEN_SWEEP_BUCKET_ASSIGNER_STATE_MACHINE_CELL);
    }

    @Test
    public void differentShardsAndStrategiesMapToDifferentSweepBucketPointerCells() {
        Stream<ShardAndStrategy> shardsAndStrategies = Stream.of(
                ShardAndStrategy.conservative(0),
                ShardAndStrategy.conservative(1),
                ShardAndStrategy.conservative(255),
                ShardAndStrategy.thorough(0),
                ShardAndStrategy.thorough(1),
                ShardAndStrategy.thorough(212),
                ShardAndStrategy.nonSweepable());

        List<Cell> cells = shardsAndStrategies
                .map(SweepAssignedBucketStoreKeyPersister.INSTANCE::sweepBucketPointerCell)
                .collect(Collectors.toList());

        assertThat(cells).doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @MethodSource("goldenSweepBucketPointerCells")
    public void shardsAndStrategiesMapToHistoricSweepBucketPointerCells(
            ShardAndStrategy shardAndStrategy, Cell expectedCell) {
        Cell actualCell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketPointerCell(shardAndStrategy);
        assertThat(actualCell).isEqualTo(expectedCell);
    }

    @Test
    public void differentBucketIdentifiersMapToDifferentSweepBucketRecordsCells() {
        Set<Integer> bucketIdentifiers = Set.of(0, 1, 123123, Integer.MAX_VALUE);
        List<Cell> cells = bucketIdentifiers.stream()
                .map(SweepAssignedBucketStoreKeyPersister.INSTANCE::sweepBucketRecordsCell)
                .collect(Collectors.toList());
        assertThat(cells).doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @MethodSource("goldenSweepBucketRecordsCells")
    public void bucketIdentifiersMapToHistoricSweepBucketRecordsCells(long bucketIdentifier, Cell expectedCell) {
        Cell actualCell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier);
        assertThat(actualCell).isEqualTo(expectedCell);
    }

    // TODO(mdaudali):  Add validation to ensure that bucket identifier for bucket and passed in above >= 0 in a
    //  separate PR.

    @Test
    // A more rigorous test would likely involve some generator for the various inputs into each cell
    // Perhaps an interesting place to leverage property-based testing
    public void cellsDoNotOverlap() {
        List<Cell> cells = Stream.concat(
                        Stream.concat(
                                Stream.of(
                                        SweepAssignedBucketStoreKeyPersister.INSTANCE
                                                .sweepBucketAssignerStateMachineCell(),
                                        SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(0)),
                                buckets().stream()
                                        .map(SweepAssignedBucketStoreKeyPersister.INSTANCE::sweepBucketsCell)),
                        Stream.of(SweeperStrategy.values())
                                .map(v -> SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketPointerCell(
                                        ShardAndStrategy.of(0, v))))
                .collect(Collectors.toList());
        assertThat(cells).doesNotHaveDuplicates();
    }

    private static Set<Bucket> buckets() {
        Set<Integer> shardsAndBucketIdentifiers = Set.of(0, 99, 100, Integer.MAX_VALUE);
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.values());
        Set<ShardAndStrategy> shardAndStrategies = Sets.cartesianProduct(shardsAndBucketIdentifiers, strategies)
                .collect(pair -> ShardAndStrategy.of(pair.getOne(), pair.getTwo()))
                .toSet();
        return Sets.cartesianProduct(shardAndStrategies, shardsAndBucketIdentifiers)
                .collect(pair -> Bucket.of(pair.getOne(), pair.getTwo()))
                .toSet();
    }

    private static Set<SweepableBucket> sweepableBuckets() {
        Set<TimestampRange> timestampRanges =
                Set.of(TimestampRange.openBucket(13123), TimestampRange.of(901273, Long.MAX_VALUE));
        return Sets.cartesianProduct(buckets(), timestampRanges)
                .collect(pair -> SweepableBucket.of(pair.getOne(), pair.getTwo()))
                .toSet();
    }

    private static Stream<Arguments> goldenSweepBucketsCells() {
        return GOLDEN_BUCKETS.entrySet().stream().map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    private static Stream<Arguments> goldenSweepBucketCellsAndValues() {
        return GOLDEN_BUCKETS.entrySet().stream().flatMap(entry -> {
            Bucket bucket = entry.getKey();
            Cell cell = entry.getValue();
            return GOLDEN_TIMESTAMP_RANGES.entrySet().stream()
                    .map(timestampRangeEntry -> Arguments.of(
                            SweepableBucket.of(bucket, timestampRangeEntry.getKey()),
                            cell,
                            timestampRangeEntry.getValue()));
        });
    }

    private static Stream<Arguments> goldenSweepBucketPointerCells() {
        return GOLDEN_SWEEP_BUCKET_POINTER_CELLS.entrySet().stream()
                .map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    // TODO(mdaudali): consider making a custom MapSource for parameterized tests
    private static Stream<Arguments> goldenSweepBucketRecordsCells() {
        return GOLDEN_SWEEP_BUCKET_RECORDS_CELLS.entrySet().stream()
                .map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    private static Cell fromBase64(String rowName, String columnName) {
        return Cell.create(
                BaseEncoding.base64().decode(rowName), BaseEncoding.base64().decode(columnName));
    }
}
