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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DefaultSweepBucketStore implements SweepBucketStore {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSweepBucketStore.class);
    private static final TableReference TABLE_REFERENCE = TargetedSweepTableFactory.of().getSweepBucketsTable(null).getTableRef();

    private final KeyValueService keyValueService;
    private final SweepableBucketRangeSerializer rangeSerializer;

    private DefaultSweepBucketStore(KeyValueService keyValueService, SweepableBucketRangeSerializer rangeSerializer) {
        this.keyValueService = keyValueService;
        this.rangeSerializer = rangeSerializer;
    }

    public static SweepBucketStore create(KeyValueService keyValueService, ObjectMapper mapper) {
        return new DefaultSweepBucketStore(keyValueService,
                SweepableBucketRangeSerializer.create(mapper));
    }

    @Override
    public Optional<SweepableBucketRange> getBucketRange(long bucketIdentifier) {
        Cell bucketCell = BucketIdentifierCellTranslator.INSTANCE.getCellForBucket(bucketIdentifier);
        Optional<byte[]> databaseBytes = readCellFromDatabase(bucketCell);
        return databaseBytes
                .map(rangeSerializer::deserialize);
    }

    @Override
    public List<Long> getFirstLiveBuckets(long lowerBoundTimestamp, int limit) {
        return List.of();
    }

    @Override
    public boolean trySetBucketRange(long bucketIdentifier, SweepableBucketRange range) {
        Cell bucketCell = BucketIdentifierCellTranslator.INSTANCE.getCellForBucket(bucketIdentifier);
        Optional<byte[]> existingData = readCellFromDatabase(bucketCell);
        if (existingData.isPresent()) {
            SweepableBucketRange existingBucketRange = rangeSerializer.deserialize(existingData.get());
        }

        byte[] newSerializedRange = rangeSerializer.serialize(range);

        CheckAndSetRequest checkAndSetRequest = getCheckAndSetRequest(bucketCell, existingData, newSerializedRange);
        try {
            keyValueService.checkAndSet(checkAndSetRequest);
            return true;
        } catch (CheckAndSetException e) {
            log.info("Attempted to set a bucket range, but was not able to do so.",
                    SafeArg.of("bucketIdentifier", bucketIdentifier),
                    SafeArg.of("bucketRange", range),
                    e);
            return false;
        }
    }

    private CheckAndSetRequest getCheckAndSetRequest(
            Cell bucketCell,
            Optional<byte[]> currentRangeForBucket,
            byte[] newSerializedRange) {
        if (currentRangeForBucket.isEmpty()) {
            return CheckAndSetRequest.newCell(TABLE_REFERENCE, bucketCell, newSerializedRange);
        } else {
            return CheckAndSetRequest.singleCell(TABLE_REFERENCE, bucketCell, currentRangeForBucket.get(), newSerializedRange);
        }
    }

    @Override
    public void deleteBucket(long bucketIdentifier) {
        keyValueService.deleteFromAtomicTable(TABLE_REFERENCE,
                ImmutableSet.of(BucketIdentifierCellTranslator.INSTANCE.getCellForBucket(bucketIdentifier)));
    }

    private Optional<byte[]> readCellFromDatabase(Cell bucketCell) {
        Map<Cell, Value> databaseValue = keyValueService.get(TABLE_REFERENCE, ImmutableMap.of(
                bucketCell,
                Long.MAX_VALUE));
        return Optional.ofNullable(databaseValue.get(bucketCell))
                .map(Value::getContents);
    }
}
