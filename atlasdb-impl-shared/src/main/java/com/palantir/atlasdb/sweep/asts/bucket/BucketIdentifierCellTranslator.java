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

package com.palantir.atlasdb.sweep.asts.bucket;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.schema.generated.SweepBucketsTable;
import com.palantir.atlasdb.schema.generated.SweepBucketsTable.SweepBucketsColumn;
import com.palantir.atlasdb.schema.generated.SweepBucketsTable.SweepBucketsRow;

/**
 * The responsibility of this class is translating bucket identifiers into entries in the
 * {@link com.palantir.atlasdb.schema.generated.SweepBucketsTable}, to allow for reasonably efficient implementation
 * of the methods of a {@link SweepBucketStore}.
 *
 * Exercise extreme caution in making changes to this class without a migration.
 */
enum BucketIdentifierCellTranslator {
    INSTANCE;

    // SweepableBucketRange is likely to be bounded above by 100 bytes.
    // 1000 here means that a single partition is no wider than 100 KiB, and also that an "empty" range query read
    // will not read more than 1000 tombstones.
    final static long BUCKET_IDENTIFIERS_PER_ROW = 1_000;

    Cell getCellForBucket(long bucketIdentifier) {
        return Cell.create(
                getRowForBucket(bucketIdentifier).persistToBytes(),
                getColumnForBucket(bucketIdentifier).persistToBytes());
    }

    long getBucketForCell(Cell cell) {
        long majorIdentifier = SweepBucketsRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()).getMajorBucketIdentifier();
        long minorIdentifier = SweepBucketsColumn.BYTES_HYDRATOR.hydrateFromBytes(cell.getColumnName()).getMinorBucketIdentifier();
        return majorIdentifier * BUCKET_IDENTIFIERS_PER_ROW + minorIdentifier;
    }

    private SweepBucketsTable.SweepBucketsRow getRowForBucket(long bucketIdentifier) {
        return SweepBucketsRow.of(bucketIdentifier / BUCKET_IDENTIFIERS_PER_ROW);
    }

    private SweepBucketsTable.SweepBucketsColumn getColumnForBucket(long bucketIdentifier) {
        return SweepBucketsColumn.of(bucketIdentifier % BUCKET_IDENTIFIERS_PER_ROW);
    }
}
