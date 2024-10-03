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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepAssignedBucketsTable.SweepAssignedBucketsRow;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class DefaultSweepAssignedBucketStore
        implements SweepBucketsTable,
                SweepBucketPointerTable,
                SweepBucketAssignerStateMachineTable,
                SweepBucketRecordsTable {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSweepAssignedBucketStore.class);
    private static final int CAS_ATTEMPT_LIMIT = 5; // TODO: Remove this and share the logic somewhere.
    private static final TimestampRangeDelete DELETE_ALL_TIMESTAMPS = new TimestampRangeDelete.Builder()
            .deleteSentinels(true)
            .timestamp(Long.MAX_VALUE)
            .endInclusive(true)
            .build();

    @VisibleForTesting
    static final TableReference TABLE_REF =
            TargetedSweepTableFactory.of().getSweepAssignedBucketsTable(null).getTableRef();

    private final KeyValueService keyValueService;
    private final ObjectPersister<TimestampRange> timestampRangePersister;
    private final ObjectPersister<BucketStateAndIdentifier> bucketStateAndIdentifierPersister;
    private final ObjectPersister<Long> bucketIdentifierPersister;

    private DefaultSweepAssignedBucketStore(
            KeyValueService keyValueService,
            ObjectPersister<TimestampRange> timestampRangePersister,
            ObjectPersister<BucketStateAndIdentifier> bucketStateAndIdentifierPersister,
            ObjectPersister<Long> bucketIdentifierPersister) {
        this.keyValueService = keyValueService;
        this.timestampRangePersister = timestampRangePersister;
        this.bucketStateAndIdentifierPersister = bucketStateAndIdentifierPersister;
        this.bucketIdentifierPersister = bucketIdentifierPersister;
    }

    static DefaultSweepAssignedBucketStore create(KeyValueService keyValueService) {
        ObjectMapper smileMapper = ObjectMappers.newServerSmileMapper();
        return new DefaultSweepAssignedBucketStore(
                keyValueService,
                ObjectPersister.of(smileMapper, TimestampRange.class, LogSafety.SAFE),
                ObjectPersister.of(smileMapper, BucketStateAndIdentifier.class, LogSafety.SAFE),
                ObjectPersister.of(smileMapper, Long.class, LogSafety.SAFE));
    }

    public void setInitialStateForBucketAssigner(long bucketIdentifier, long startTimestamp) {
        BucketStateAndIdentifier initialState =
                BucketStateAndIdentifier.of(bucketIdentifier, BucketAssignerState.start(startTimestamp));
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        casCell(cell, Optional.empty(), bucketStateAndIdentifierPersister.trySerialize(initialState));
    }

    @Override
    // TODO: What happens when there was no state originally?
    //  Do we bootstrap it separately, or do we force the caller of getBucketStateAndIdentifier to handle that case.
    public void updateStateMachineForBucketAssigner(
            BucketStateAndIdentifier original, BucketStateAndIdentifier updated) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        casCell(
                cell,
                Optional.of(bucketStateAndIdentifierPersister.trySerialize(original)),
                bucketStateAndIdentifierPersister.trySerialize(updated));
    }

    @Override
    public BucketStateAndIdentifier getBucketStateAndIdentifier() {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        Optional<BucketStateAndIdentifier> value = readCell(cell, bucketStateAndIdentifierPersister::tryDeserialize);
        return value.orElseThrow(() -> new SafeIllegalStateException(
                "No bucket state and identifier found. This should have been bootstrapped during "
                        + " initialisation, and as such, is an invalid state."));
    }

    @Override
    public Set<Bucket> getStartingBucketsForShards(Set<ShardAndStrategy> shardAndStrategies) {
        List<Cell> cellsToLoad = shardAndStrategies.stream()
                .map(SweepAssignedBucketStoreKeyPersister.INSTANCE::sweepBucketPointerCell)
                .collect(Collectors.toList());
        Map<Cell, Value> result = keyValueService.getRows(
                TABLE_REF,
                cellsToLoad.stream().map(Cell::getRowName).collect(Collectors.toList()),
                // TODO: This is janky.

                ColumnSelection.create(
                        cellsToLoad.stream().map(Cell::getColumnName).collect(Collectors.toSet())),
                Long.MAX_VALUE);

        return shardAndStrategies.stream()
                .map(shardAndStrategy -> {
                    Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketPointerCell(shardAndStrategy);
                    return Optional.ofNullable(result.get(cell))
                            .map(Value::getContents)
                            .map(bucketIdentifierPersister::tryDeserialize)
                            .map(bucketIdentifier -> Bucket.of(shardAndStrategy, bucketIdentifier))
                            .orElseGet(() -> {
                                log.warn(
                                        "No starting bucket found for shard and strategy {}",
                                        SafeArg.of("shardAndStrategy", shardAndStrategy));
                                return Bucket.of(shardAndStrategy, 0);
                            });
                })
                .collect(Collectors.toSet());
    }

    @Override
    // TODO: See if we should centralise the work that was done in Jeremy's PR
    public void updateStartingBucketForShardAndStrategy(Bucket newStartingBucket) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketPointerCell(
                newStartingBucket.shardAndStrategy());

        byte[] serializedBucketProgress = bucketIdentifierPersister.trySerialize(newStartingBucket.bucketIdentifier());
        for (int attempt = 0; attempt < CAS_ATTEMPT_LIMIT; attempt++) {
            try {
                Optional<Long> currentStartingBucket = readCell(cell, bucketIdentifierPersister::tryDeserialize);
                if (currentStartingBucket.isEmpty()) {
                    keyValueService.checkAndSet(CheckAndSetRequest.newCell(TABLE_REF, cell, serializedBucketProgress));
                    if (log.isDebugEnabled()) {
                        log.debug("Persisted new starting bucket", SafeArg.of("newStartingBucket", newStartingBucket));
                    }
                } else {
                    if (newStartingBucket.bucketIdentifier() > currentStartingBucket.get()) {
                        keyValueService.checkAndSet(CheckAndSetRequest.singleCell(
                                TABLE_REF,
                                cell,
                                bucketIdentifierPersister.trySerialize(currentStartingBucket.get()),
                                bucketIdentifierPersister.trySerialize(newStartingBucket.bucketIdentifier())));
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "Updated sweep bucket progress",
                                    SafeArg.of("previousStartingBucket", currentStartingBucket),
                                    SafeArg.of("newStartingBucket", newStartingBucket));
                        }
                    } else {
                        log.info(
                                "Attempted to update starting bucket, but the existing starting bucket in the database"
                                        + " was already ahead of us (possible timelock lost lock?)",
                                SafeArg.of("previousStartingBucket", currentStartingBucket),
                                SafeArg.of("newStartingBucket", newStartingBucket));
                    }
                }
                return;
            } catch (RuntimeException e) {
                if (attempt == CAS_ATTEMPT_LIMIT - 1) {
                    log.warn(
                            "Repeatedly failed to update starting bucket as part of sweep; throwing, some work may"
                                    + " need to be re-done.",
                            SafeArg.of("attemptedNewStartingBucket", newStartingBucket),
                            SafeArg.of("numAttempts", CAS_ATTEMPT_LIMIT),
                            e);
                    throw e;
                } else {
                    log.info(
                            "Failed to read or update starting bucket as part of sweep. Retrying",
                            SafeArg.of("newStartingBucket", newStartingBucket),
                            SafeArg.of("attemptNumber", attempt + 1),
                            e);
                }
            }
        }
    }

    @Override
    // TODO: This is also not simple - we need to keep loading rows until we load n live elements.
    //  and also need to cap it at the latest bucket identifier.

    // We assume here that each bucket is for a unique shard and strategy

    // Do we actually need to do this? At the time we were going to load 50 buckets which were fine partitions, now
    // they're 10 minutes long. 50 x 10 minutes is 500 minutes, so almost 10 hours worth of data each time. In a normal
    // case where startbuckets is near the current bucket identifier, we would shortcircuit, but the algorithm for
    // iteratively
    // loading optimal is a little bit more complex. Is it sufficient to just load the row of the startBucket (and maybe
    // the row after it)
    // and return everything there? There may be no buckets (if startBucket hasn't updated yet), but this should be
    // short lived (assuming the background task is running).
    // This however does add a dependence on the background task actually running, however, whereas a model that keeps
    // searching until we load n buckets or reach the end
    // would work even if the background task does not run.

    // TODO:
    //  Do an iteration of all startMajorBucketIdentifierByShardAndStrategy
    //  Add to set, and then decrement from maxRemainingBucketsToLoad.
    //  If less than 0, then remove from startMajorBucketIdentifierByShardAndStrategy.
    //  Cap the major identifier to the major equivalent of the maxBucketIdentifier.
    //  Repeat until complete
    public Set<SweepableBucket> getSweepableBuckets(Set<Bucket> startBuckets) {
        List<SweepAssignedBucketsRow> rows = startBuckets.stream()
                .flatMap(bucket -> Stream.of(
                        SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsRow(bucket),
                        SweepAssignedBucketStoreKeyPersister.INSTANCE.nextSweepBucketsRow(bucket)))
                .collect(Collectors.toList());

        return readRows(rows);
    }

    @Override
    public void putTimestampRangeForBucket(
            Bucket bucket, Optional<TimestampRange> oldTimestampRange, TimestampRange newTimestampRange) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsCell(bucket);
        casCell(
                cell,
                oldTimestampRange.map(timestampRangePersister::trySerialize),
                timestampRangePersister.trySerialize(newTimestampRange));
    }

    @Override
    public void deleteBucketEntry(Bucket bucket) {
        deleteCell(SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsCell(bucket));
    }

    @Override
    public TimestampRange getTimestampRangeRecord(long bucketIdentifier) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier);
        return readCell(cell, timestampRangePersister::tryDeserialize)
                .orElseThrow(() -> new SafeIllegalStateException(
                        "No timestamp range record found for bucket identifier",
                        SafeArg.of("bucketIdentifier", bucketIdentifier)));
    }

    @Override
    public void putTimestampRangeRecord(long bucketIdentifier, TimestampRange timestampRange) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier);
        casCell(cell, Optional.empty(), timestampRangePersister.trySerialize(timestampRange));
    }

    @Override
    public void deleteTimestampRangeRecord(long bucketIdentifier) {
        deleteCell(SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier));
    }

    private void casCell(Cell cell, Optional<byte[]> existingValue, byte[] newValue) {
        CheckAndSetRequest request = existingValue
                .map(value -> CheckAndSetRequest.singleCell(TABLE_REF, cell, value, newValue))
                .orElseGet(() -> CheckAndSetRequest.newCell(TABLE_REF, cell, newValue));
        keyValueService.checkAndSet(request);
    }

    private <T> Optional<T> readCell(Cell cell, Function<byte[], T> deserializer) {
        Map<Cell, Value> values = keyValueService.get(TABLE_REF, ImmutableMap.of(cell, Long.MAX_VALUE));
        System.out.println(values);
        Optional<byte[]> value = Optional.ofNullable(values.get(cell)).map(Value::getContents);
        return value.map(deserializer);
    }

    private void deleteCell(Cell cell) {
        // TODO(mdaudali): This is sneaky, in that we know (or believe, and tests will confirm), for now, that the
        // timestamp written for the CAS is always 0, and so we can simply issue the delete to 0.
        // If the CAS timestamp was to change, then this test would break.

        // DO NOT CHANGE from 0L without understanding the implications. You likely need to move to a rangeDelete to
        // delete all timestamps

        // LATER change - I actually don't think we can do a point delete of TS 0, because the CAS will have a different
        // CASSANDRA timestamp, and so by last write wins, the delete won't apply.

        // Instead, I think we need to do a deleteAllTimestamps A LA sweep.

        // We know that no one should ever write to a cell that was deleted, and so applying a giant range tombstone
        // is still correct.
        keyValueService.deleteAllTimestamps(TABLE_REF, ImmutableMap.of(cell, DELETE_ALL_TIMESTAMPS));
    }

    private Set<SweepableBucket> readRows(List<SweepAssignedBucketsRow> rows) {
        Map<Cell, Value> reads = keyValueService.getRows(
                TABLE_REF,
                rows.stream().map(SweepAssignedBucketsRow::persistToBytes).collect(Collectors.toList()),
                ColumnSelection.all(),
                Long.MAX_VALUE);
        return reads.entrySet().stream()
                .map(entry -> SweepAssignedBucketStoreKeyPersister.INSTANCE.fromSweepBucketCellAndValue(
                        entry.getKey(), entry.getValue(), timestampRangePersister))
                .collect(Collectors.toSet());
    }
}
