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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ConsistentOrderingObjectMappers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class DefaultBucketProgressStore implements BucketProgressStore {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketProgressStore.class);
    private static final int CAS_ATTEMPT_LIMIT = 10;

    public static final TableReference TABLE_REF =
            TargetedSweepTableFactory.of().getSweepBucketProgressTable(null).getTableRef();

    private final KeyValueService keyValueService;
    private final BucketProgressPersister bucketProgressPersister;

    @VisibleForTesting
    DefaultBucketProgressStore(KeyValueService keyValueService, BucketProgressPersister bucketProgressPersister) {
        this.keyValueService = keyValueService;
        this.bucketProgressPersister = bucketProgressPersister;
    }

    public static BucketProgressStore create(KeyValueService keyValueService) {
        return new DefaultBucketProgressStore(
                keyValueService, BucketProgressPersister.create(ConsistentOrderingObjectMappers.OBJECT_MAPPER));
    }

    @Override
    public Optional<BucketProgress> getBucketProgress(Bucket bucket) {
        return readBucketProgress(DefaultBucketKeySerializer.INSTANCE.bucketToCell(bucket))
                .map(bucketProgressPersister::deserializeProgress);
    }

    @Override
    public void updateBucketProgressToAtLeast(Bucket bucket, BucketProgress minimum) {
        Cell bucketCell = DefaultBucketKeySerializer.INSTANCE.bucketToCell(bucket);
        byte[] serializedBucketProgress = bucketProgressPersister.serializeProgress(minimum);
        for (int attempt = 0; attempt < CAS_ATTEMPT_LIMIT; attempt++) {
            try {
                Optional<byte[]> currentProgress = readBucketProgress(bucketCell);
                if (currentProgress.isEmpty()) {
                    keyValueService.checkAndSet(
                            CheckAndSetRequest.newCell(TABLE_REF, bucketCell, serializedBucketProgress));
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Persisted new sweep bucket {} progress to {}",
                                SafeArg.of("bucket", bucket),
                                SafeArg.of("minimumProgress", minimum));
                    }
                } else {
                    BucketProgress extantCurrentProgress =
                            bucketProgressPersister.deserializeProgress(currentProgress.get());

                    if (minimum.compareTo(extantCurrentProgress) > 0) {
                        keyValueService.checkAndSet(CheckAndSetRequest.singleCell(
                                TABLE_REF,
                                bucketCell,
                                currentProgress.get(),
                                bucketProgressPersister.serializeProgress(minimum)));
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "Updated sweep bucket {} progress from {} to {}",
                                    SafeArg.of("bucket", bucket),
                                    SafeArg.of("previousPersistedProgress", extantCurrentProgress),
                                    SafeArg.of("minimumProgress", minimum));
                        }
                    } else {
                        log.info(
                                "Attempted to update sweep bucket {} progress, but the existing progress {}"
                                        + " in the database was already ahead of us {} (possible timelock lost lock?)",
                                SafeArg.of("bucket", bucket),
                                SafeArg.of("persistedProgress", extantCurrentProgress),
                                SafeArg.of("minimumProgress", minimum));
                    }
                }
                return;
            } catch (RuntimeException e) {
                if (attempt == CAS_ATTEMPT_LIMIT - 1) {
                    log.warn(
                            "Repeatedly failed to update bucket {} progress to {} as part of sweep; throwing"
                                    + " after {} attempts, some work may need to be re-done",
                            SafeArg.of("bucket", bucket),
                            SafeArg.of("minimumProgress", minimum),
                            SafeArg.of("numAttempts", CAS_ATTEMPT_LIMIT),
                            e);
                    throw e;
                } else {
                    log.info(
                            "Failed to read or update bucket {} progress {} as part of sweep after attempt {}."
                                    + " Retrying",
                            SafeArg.of("bucket", bucket),
                            SafeArg.of("minimumProgress", minimum),
                            SafeArg.of("attemptNumber", attempt + 1),
                            e);
                }
            }
        }
    }

    @Override
    public void deleteBucketProgress(Bucket bucket) {
        Cell cell = DefaultBucketKeySerializer.INSTANCE.bucketToCell(bucket);
        keyValueService.deleteFromAtomicTable(TABLE_REF, Set.of(cell));
    }

    private Optional<byte[]> readBucketProgress(Cell cell) {
        Map<Cell, Value> values = keyValueService.get(TABLE_REF, ImmutableMap.of(cell, Long.MAX_VALUE));
        return Optional.ofNullable(values.get(cell)).map(Value::getContents);
    }
}
