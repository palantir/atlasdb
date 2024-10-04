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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

final class DefaultSweepAssignedBucketStore implements SweepBucketAssignerStateMachineTable {
    @VisibleForTesting
    static final TableReference TABLE_REF =
            TargetedSweepTableFactory.of().getSweepAssignedBucketsTable(null).getTableRef();

    private final KeyValueService keyValueService;
    private final ObjectPersister<BucketStateAndIdentifier> bucketStateAndIdentifierPersister;

    private DefaultSweepAssignedBucketStore(
            KeyValueService keyValueService,
            ObjectPersister<BucketStateAndIdentifier> bucketStateAndIdentifierPersister) {
        this.keyValueService = keyValueService;
        this.bucketStateAndIdentifierPersister = bucketStateAndIdentifierPersister;
    }

    static DefaultSweepAssignedBucketStore create(KeyValueService keyValueService) {
        return new DefaultSweepAssignedBucketStore(
                keyValueService, ObjectPersister.of(BucketStateAndIdentifier.class, LogSafety.SAFE));
    }

    @Override
    public void setInitialStateForBucketAssigner(long bucketIdentifier, long startTimestamp) {
        BucketStateAndIdentifier initialState =
                BucketStateAndIdentifier.of(bucketIdentifier, BucketAssignerState.start(startTimestamp));
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        casCell(cell, Optional.empty(), bucketStateAndIdentifierPersister.trySerialize(initialState));
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
        Cell cell = SweepAssignedBucketStoreKeyPersister.INSTANCE.sweepBucketAssignerStateMachineCell();
        Optional<BucketStateAndIdentifier> value = readCell(cell, bucketStateAndIdentifierPersister::tryDeserialize);
        return value.orElseThrow(() -> new SafeIllegalStateException(
                "No bucket state and identifier found. This should have been bootstrapped during"
                        + " initialisation, and as such, is an invalid state."));
    }

    private void casCell(Cell cell, Optional<byte[]> existingValue, byte[] newValue) {
        CheckAndSetRequest request = existingValue
                .map(value -> CheckAndSetRequest.singleCell(TABLE_REF, cell, value, newValue))
                .orElseGet(() -> CheckAndSetRequest.newCell(TABLE_REF, cell, newValue));
        keyValueService.checkAndSet(request);
    }

    private <T> Optional<T> readCell(Cell cell, Function<byte[], T> deserializer) {
        Map<Cell, Value> values = keyValueService.get(TABLE_REF, ImmutableMap.of(cell, Long.MAX_VALUE));
        Optional<byte[]> value = Optional.ofNullable(values.get(cell)).map(Value::getContents);
        return value.map(deserializer);
    }
}
