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
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepAssignedBucketsTable.SweepAssignedBucketsRow;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DefaultSweepAssignedBucketStore
        implements SweepBucketAssignerStateMachineTable,
                SweepBucketPointerTable,
                SweepBucketsTable,
                SweepBucketRecordsTable {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSweepAssignedBucketStore.class);
    private static final int CAS_ATTEMPT_LIMIT = 5;

    public static final TableReference TABLE_REF =
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

    public static DefaultSweepAssignedBucketStore create(KeyValueService keyValueService) {
        return new DefaultSweepAssignedBucketStore(
                keyValueService,
                ObjectPersister.of(TimestampRange.class, LogSafety.SAFE),
                ObjectPersister.of(BucketStateAndIdentifier.class, LogSafety.SAFE),
                ObjectPersister.of(Long.class, LogSafety.SAFE));
    }

    @Override
    public void setInitialStateForBucketAssigner(BucketAssignerState initialState) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        casCell(
                cell,
                Optional.empty(),
                bucketStateAndIdentifierPersister.trySerialize(BucketStateAndIdentifier.of(0, initialState)));
    }

    @Override
    public boolean doesStateMachineStateExist() {
        return maybeGetBucketStateAndIdentifier().isPresent();
    }

    @Override
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
        Optional<BucketStateAndIdentifier> value = maybeGetBucketStateAndIdentifier();
        return value.orElseThrow(() -> new SafeIllegalStateException(
                "No bucket state and identifier found. This should have been bootstrapped during"
                        + " initialisation, and as such, is an invalid state."));
    }

    private Optional<BucketStateAndIdentifier> maybeGetBucketStateAndIdentifier() {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        return readCell(cell, bucketStateAndIdentifierPersister::tryDeserialize);
    }

    @Override
    public Set<Bucket> getStartingBucketsForShards(Set<ShardAndStrategy> shardAndStrategies) {
        Map<ShardAndStrategy, Cell> shardAndStrategyToCell = shardAndStrategies.stream()
                .collect(Collectors.toMap(
                        Function.identity(), SweepAssignedBucketStoreKeyPersister.INSTANCE::sweepBucketPointerCell));
        Map<Cell, Long> results = readCells(shardAndStrategyToCell.values(), bucketIdentifierPersister::tryDeserialize);
        return KeyedStream.stream(shardAndStrategyToCell)
                .map(cell -> Optional.ofNullable(results.get(cell)))
                .map((shardAndStrategy, bucketIdentifier) -> bucketIdentifier
                        .map(id -> Bucket.of(shardAndStrategy, id))
                        .orElseGet(() -> {
                            log.warn(
                                    "No starting bucket found for shard and strategy {}. Starting from bucket"
                                            + " 0 for this shard. This is expected the first time the service starts up"
                                            + " using this version of sweep, or if the number of shards has changed.",
                                    SafeArg.of("shardAndStrategy", shardAndStrategy));
                            return Bucket.of(shardAndStrategy, 0);
                        }))
                .values()
                .collect(Collectors.toSet());
    }

    @Override
    public void updateStartingBucketForShardAndStrategy(Bucket newStartingBucket) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketPointerCell(
                newStartingBucket.shardAndStrategy());

        byte[] serializedBucketProgress = bucketIdentifierPersister.trySerialize(newStartingBucket.bucketIdentifier());
        for (int attempt = 1; attempt <= CAS_ATTEMPT_LIMIT; attempt++) {
            try {
                Optional<Long> currentStartingBucket = readCell(cell, bucketIdentifierPersister::tryDeserialize);
                if (currentStartingBucket.isEmpty()) {
                    keyValueService.checkAndSet(CheckAndSetRequest.newCell(TABLE_REF, cell, serializedBucketProgress));
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Persisted new starting bucket {}", SafeArg.of("newStartingBucket", newStartingBucket));
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
                                    "Updated sweep bucket progress from {} to {}",
                                    SafeArg.of("previousStartingBucket", currentStartingBucket),
                                    SafeArg.of("newStartingBucket", newStartingBucket));
                        }
                    } else {
                        log.info(
                                "Attempted to update starting bucket from {} to {}, but the existing starting"
                                        + " bucket in the database was already ahead of us"
                                        + " (possible timelock lost lock?)",
                                SafeArg.of("previousStartingBucket", currentStartingBucket),
                                SafeArg.of("newStartingBucket", newStartingBucket));
                    }
                }
                return;
            } catch (RuntimeException e) {
                if (attempt == CAS_ATTEMPT_LIMIT) {
                    log.warn(
                            "Repeatedly failed to update starting bucket to {} as part of sweep; throwing"
                                    + " after {} attempts, some work may need to be re-done",
                            SafeArg.of("attemptedNewStartingBucket", newStartingBucket),
                            SafeArg.of("numAttempts", CAS_ATTEMPT_LIMIT),
                            e);
                    throw e;
                } else {
                    log.info(
                            "Failed to read or update starting bucket to {} as part of sweep after attempt {}."
                                    + " Retrying",
                            SafeArg.of("newStartingBucket", newStartingBucket),
                            SafeArg.of("attemptNumber", attempt),
                            e);
                }
            }
        }
    }

    @Override
    // This implementation assumes that the background task will eventually (but relatively quickly) keep the starting
    // buckets up to date, such that we won't end up loading no buckets for an extended period of time.
    // If we assume that the starting bucket is relatively up to date, then by loading the row containing said starting
    // bucket + the next one, we should get some buckets to work on at all times (if buckets are present).
    // This does suffer from head of line blocking, in that we could end up just loading the starting bucket and no
    // other bucket (say, they've already been swept), and, if the starting bucket cannot be swept right now (e.g.,
    // not enough DB nodes online to perform the deletion), then we won't handle any other work.
    // It'd likely need to be an extended period of time in which we cannot sweep the starting bucket because
    // we'd need to saturate and then sweep the entirety of the next row of buckets. In the implementation at the
    // time of writing, that's 100 * 10 minutes = 1000 minute = 17 hours.

    // An alternative, but far more complex implementation, is to repeatedly load rows until you've found N _live_
    // buckets (or know that there are no more buckets, using the state machine information). We opted not to implement
    // this because it's more complex and unnecessary without evidence of the simple implementation being a problem
    public Set<SweepableBucket> getSweepableBuckets(Set<Bucket> startBuckets) {
        List<SweepAssignedBucketsRow> rows = startBuckets.stream()
                .flatMap(bucket -> Stream.of(
                        SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsRow(bucket),
                        SweepAssignedBucketStoreKeyPersister.INSTANCE.nextSweepBucketsRow(bucket)))
                .collect(Collectors.toList());

        return readSweepableBucketRows(rows);
    }

    @Override
    public Optional<SweepableBucket> getSweepableBucket(Bucket bucket) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsCell(bucket);
        return readCell(
                cell,
                (byte[] bytes) -> SweepAssignedBucketStoreKeyPersister.INSTANCE.fromSweepBucketCellAndValue(
                        cell, Value.create(bytes, -1), timestampRangePersister));
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
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketsCell(bucket);
        deleteCell(cell);
    }

    @Override
    public TimestampRange getTimestampRangeRecord(long bucketIdentifier) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier);
        return readCell(cell, timestampRangePersister::tryDeserialize)
                .orElseThrow(() -> new NoSuchElementException("No timestamp range record found for bucket identifier"));
    }

    @Override
    public void putTimestampRangeRecord(long bucketIdentifier, TimestampRange timestampRange) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier);
        casCell(cell, Optional.empty(), timestampRangePersister.trySerialize(timestampRange));
    }

    @Override
    public void deleteTimestampRangeRecord(long bucketIdentifier) {
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketRecordsCell(bucketIdentifier);
        deleteCell(cell);
    }

    private void deleteCell(Cell cell) {
        keyValueService.deleteFromAtomicTable(TABLE_REF, Set.of(cell));
    }

    private void casCell(Cell cell, Optional<byte[]> existingValue, byte[] newValue) {
        CheckAndSetRequest request = existingValue
                .map(value -> CheckAndSetRequest.singleCell(TABLE_REF, cell, value, newValue))
                .orElseGet(() -> CheckAndSetRequest.newCell(TABLE_REF, cell, newValue));
        keyValueService.checkAndSet(request);
    }

    private <T> Optional<T> readCell(Cell cell, Function<byte[], T> deserializer) {
        Map<Cell, T> cells = readCells(Set.of(cell), deserializer);
        return Optional.ofNullable(cells.get(cell));
    }

    private <T> Map<Cell, T> readCells(Collection<Cell> cells, Function<byte[], T> deserializer) {
        Map<Cell, Value> values = keyValueService.get(
                TABLE_REF, cells.stream().collect(Collectors.toMap(Function.identity(), cell -> Long.MAX_VALUE)));
        return values.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> deserializer.apply(entry.getValue().getContents())));
    }

    private Set<SweepableBucket> readSweepableBucketRows(List<SweepAssignedBucketsRow> rows) {
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
