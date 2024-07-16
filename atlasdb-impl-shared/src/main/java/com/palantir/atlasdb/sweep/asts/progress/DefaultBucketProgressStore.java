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

package com.palantir.atlasdb.sweep.asts.progress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepBucketProgressTable;
import com.palantir.atlasdb.schema.generated.SweepBucketProgressTable.SweepBucketProgressNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepBucketProgressTable.SweepBucketProgressRow;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;

public class DefaultBucketProgressStore implements BucketProgressStore {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketProgressStore.class);
    private static final int CAS_ATTEMPT_LIMIT = 10;

    @VisibleForTesting
    static final TableReference TABLE_REF =
            TargetedSweepTableFactory.of().getSweepBucketProgressTable(null).getTableRef();

    // I know, this is kind of suboptimal given our TKVS initiative elsewhere...
    private final KeyValueService kvs;

    public DefaultBucketProgressStore(KeyValueService kvs) {
        this.kvs = kvs;
    }

    @Override
    public Optional<BucketProgress> getBucketProgress(SweepableBucket bucket) {
        return readBucketProgress(bucketToCell(bucket)).map(BucketProgressSerialization::deserialize);
    }

    @Override
    public void updateBucketProgressToAtLeast(SweepableBucket bucket, BucketProgress bucketProgress) {
        Cell bucketCell = bucketToCell(bucket);
        byte[] serializedBucketProgress = BucketProgressSerialization.serialize(bucketProgress);
        for (int attempt = 0; attempt < CAS_ATTEMPT_LIMIT; attempt++) {
            try {
                Optional<byte[]> currentProgress = readBucketProgress(bucketCell);
                if (currentProgress.isEmpty()) {
                    kvs.checkAndSet(CheckAndSetRequest.newCell(TABLE_REF, bucketCell, serializedBucketProgress));
                } else {
                    BucketProgress extantCurrentProgress =
                            BucketProgressSerialization.deserialize(currentProgress.get());

                    if (bucketProgress.compareTo(extantCurrentProgress) > 0) {
                        kvs.checkAndSet(CheckAndSetRequest.singleCell(
                                TABLE_REF,
                                bucketCell,
                                currentProgress.get(),
                                BucketProgressSerialization.serialize(bucketProgress)));
                    }
                }
            } catch (RuntimeException e) {
                if (attempt == CAS_ATTEMPT_LIMIT - 1) {
                    log.warn(
                            "Repeatedly failed to update bucket progress as part of sweep; throwing, some work may"
                                    + " need to be re-done.",
                            SafeArg.of("bucket", bucket),
                            SafeArg.of("bucketProgress", bucketProgress),
                            SafeArg.of("numAttempts", CAS_ATTEMPT_LIMIT),
                            e);
                    throw e;
                } else {
                    log.info(
                            "Failed to read or update bucket progress as part of sweep. Retrying",
                            SafeArg.of("bucket", bucket),
                            SafeArg.of("bucketProgress", bucketProgress),
                            e);
                }
            }
        }
    }

    @Override
    public void markBucketComplete(SweepableBucket bucket) {
        // Think carefully about the order of manipulating this table and sweepable timestamps!
        // TODO (jkong): KVS delete is NOT the right endpoint as far as C* is concerned.
        // Need to add support for deleteWithTimestamp, deleteAtomic, or similar.
        kvs.deleteFromAtomicTable(TABLE_REF, ImmutableSet.of(bucketToCell(bucket)));
    }

    private Optional<byte[]> readBucketProgress(Cell cell) {
        Map<Cell, Value> values = kvs.get(TABLE_REF, ImmutableMap.of(cell, Long.MAX_VALUE));
        return Optional.ofNullable(values.get(cell)).map(Value::getContents);
    }

    private static Cell bucketToCell(SweepableBucket bucket) {
        SweepBucketProgressTable.SweepBucketProgressRow row = SweepBucketProgressRow.of(
                bucket.shardAndStrategy().shard(),
                bucket.bucketIdentifier(),
                persistStrategy(bucket.shardAndStrategy().strategy()));
        return Cell.create(row.persistToBytes(), SweepBucketProgressNamedColumn.BUCKET_PROGRESS.getShortName());
    }

    private static byte[] persistStrategy(SweeperStrategy strategy) {
        switch (strategy) {
            case THOROUGH:
                return new byte[]{0};
            case CONSERVATIVE:
                return new byte[]{1};
            case NON_SWEEPABLE:
                return new byte[]{2};
            default:
                throw new SafeIllegalStateException(
                        "Unexpected sweeper strategy", SafeArg.of("sweeperStrategy", strategy));
        }
    }
}
