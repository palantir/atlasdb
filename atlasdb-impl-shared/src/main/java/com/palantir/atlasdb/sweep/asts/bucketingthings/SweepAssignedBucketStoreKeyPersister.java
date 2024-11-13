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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.schema.generated.SweepAssignedBucketsTable.SweepAssignedBucketsColumn;
import com.palantir.atlasdb.schema.generated.SweepAssignedBucketsTable.SweepAssignedBucketsRow;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;

/**
 * The layout for this table is:
 *
 * +--------------------------+---------------+-----------------+-----------------+-----------------+
 * |           Row            |    Col -1     |        0        |        1        |        X        |
 * +--------------------------+---------------+-----------------+-----------------+-----------------+
 * | (-1, -1, -1)             | STATE_MACHINE |                 |                 |                 |
 * | (shard, -1, strategy)    | START_BUCKET  |                 |                 |                 |
 * | (-1, major, -1)          |               | TIMESTAMP_RANGE | TIMESTAMP_RANGE | TIMESTAMP_RANGE |
 * | (shard, major, strategy) |               | BUCKET          | BUCKET          | BUCKET          |
 * +--------------------------+---------------+-----------------+-----------------+-----------------+
 * Where shard, strategy, major are non-negative.
 *
 *
 */
enum SweepAssignedBucketStoreKeyPersister {
    INSTANCE;

    private static final long ROW_LENGTH = 100;
    private static final byte RESERVED_IDENTIFIER = -1;

    private static final byte[] RESERVED_STRATEGY = new byte[] {RESERVED_IDENTIFIER};
    private static final byte[] RESERVED_COLUMN =
            SweepAssignedBucketsColumn.of(RESERVED_IDENTIFIER).persistToBytes();
    private static final Cell SWEEP_BUCKET_ASSIGNER_STATE_MACHINE_CELL = Cell.create(
            SweepAssignedBucketsRow.of(RESERVED_IDENTIFIER, RESERVED_IDENTIFIER, RESERVED_STRATEGY)
                    .persistToBytes(),
            RESERVED_COLUMN);

    Cell sweepBucketsCell(Bucket bucket) {
        SweepAssignedBucketsRow row = sweepBucketsRow(bucket);
        SweepAssignedBucketsColumn column =
                SweepAssignedBucketsColumn.of(minorBucketIdentifier(bucket.bucketIdentifier()));
        return Cell.create(row.persistToBytes(), column.persistToBytes());
    }

    SweepAssignedBucketsRow sweepBucketsRow(Bucket bucket) {
        return SweepAssignedBucketsRow.of(
                bucket.shardAndStrategy().shard(),
                majorBucketIdentifier(bucket.bucketIdentifier()),
                bucket.shardAndStrategy().strategy().persistToBytes());
    }

    SweepAssignedBucketsRow nextSweepBucketsRow(Bucket bucket) {
        SweepAssignedBucketsRow currentRow = sweepBucketsRow(bucket);
        return SweepAssignedBucketsRow.of(
                currentRow.getShard(), currentRow.getMajorBucketIdentifier() + 1, currentRow.getStrategy());
    }

    SweepableBucket fromSweepBucketCellAndValue(
            Cell cell, byte[] value, ObjectPersister<TimestampRange> timestampRangePersister) {
        SweepAssignedBucketsRow row = SweepAssignedBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName());
        SweepAssignedBucketsColumn column =
                SweepAssignedBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(cell.getColumnName());
        TimestampRange timestampRange = timestampRangePersister.tryDeserialize(value);
        int shard = Math.toIntExact(row.getShard()); // throws if invalid shard
        return SweepableBucket.of(
                Bucket.of(
                        ShardAndStrategy.of(shard, SweeperStrategy.BYTES_HYDRATOR.hydrateFromBytes(row.getStrategy())),
                        bucketIdentifier(row.getMajorBucketIdentifier(), column.getMinorBucketIdentifier())),
                timestampRange);
    }

    Cell sweepBucketAssignerStateMachineCell() {
        return SWEEP_BUCKET_ASSIGNER_STATE_MACHINE_CELL;
    }

    Cell sweepBucketPointerCell(ShardAndStrategy shardAndStrategy) {
        SweepAssignedBucketsRow row = SweepAssignedBucketsRow.of(
                shardAndStrategy.shard(),
                RESERVED_IDENTIFIER,
                shardAndStrategy.strategy().persistToBytes());
        return Cell.create(row.persistToBytes(), RESERVED_COLUMN);
    }

    // This is _not_ keyed on the shard and strategy, which does make clean up harder, because you need to ensure
    // that no one, across all of the shards and strategies, will use the value.
    // We estimate this to be around 25MB of data per keyspace per year, which we believe is fine to abandon.
    // If you do want to have the ability to retention the data, consider a one off task to clean up old data, or
    // to _not_ delete the bucket in the foreground cleaner, but instead to mark it as deleted and actually delete it
    // in the background task
    Cell sweepBucketRecordsCell(long bucketIdentifier) {
        SweepAssignedBucketsRow row = SweepAssignedBucketsRow.of(
                RESERVED_IDENTIFIER, majorBucketIdentifier(bucketIdentifier), RESERVED_STRATEGY);
        SweepAssignedBucketsColumn column = SweepAssignedBucketsColumn.of(minorBucketIdentifier(bucketIdentifier));
        return Cell.create(row.persistToBytes(), column.persistToBytes());
    }

    private static long bucketIdentifier(long majorBucketIdentifier, long minorBucketIdentifier) {
        return majorBucketIdentifier * ROW_LENGTH + minorBucketIdentifier;
    }

    private static long majorBucketIdentifier(long bucketIdentifier) {
        return bucketIdentifier / ROW_LENGTH;
    }

    private static long minorBucketIdentifier(long bucketIdentifier) {
        return bucketIdentifier % ROW_LENGTH;
    }
}
