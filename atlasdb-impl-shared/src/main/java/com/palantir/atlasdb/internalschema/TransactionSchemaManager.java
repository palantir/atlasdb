/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.internalschema;

import java.util.Optional;

import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.timestamp.TimestampService;

public class TransactionSchemaManager {
    private static final long ADVANCEMENT_QUANTUM = 5_000_000;

    private final CoordinationService<InternalSchemaMetadata> coordinationService;
    private final TimestampService timestampService;

    public TransactionSchemaManager(
            CoordinationService<InternalSchemaMetadata> coordinationService,
            TimestampService timestampService) {
        this.coordinationService = coordinationService;
        this.timestampService = timestampService;
    }

    public int getTransactionsSchemaVersion(long timestamp) {
        Optional<Integer> possibleVersion =
                extractTimestampVersion(coordinationService.getValueForTimestamp(timestamp), timestamp);
        while (!possibleVersion.isPresent()) {
            CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> casResult = tryPerpetuateExistingState();
            possibleVersion = extractTimestampVersion(casResult.existingValues()
                    .stream()
                    .filter(valueAndBound -> valueAndBound.bound() >= timestamp)
                    .findAny(),
                    timestamp);
        }
        return possibleVersion.get();
    }

    public void installNewTransactionsSchemaVersion(int newVersion) {
//        coordinationService.tryTransformCurrentValue(valueAndBound -> {
//            if (!valueAndBound.value().isPresent()) {
//                throw new IllegalStateException("Persisted value is empty, which is unexpected.");
//            }
//
//            InternalSchemaMetadata presentMetadata = valueAndBound.value().get();
//
//
//        });
    }

    private CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> tryPerpetuateExistingState() {
        return coordinationService.tryTransformCurrentValue(valueAndBound ->
                valueAndBound.value().orElseThrow(
                        () -> new IllegalStateException("Cannot perpetuate an existing state that didn't exist!")));
    }

    private static Optional<Integer> extractTimestampVersion(
            Optional<ValueAndBound<InternalSchemaMetadata>> valueAndBound, long timestamp) {
        return valueAndBound
                .flatMap(ValueAndBound::value)
                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                .map(rangeMap -> rangeMap.get(timestamp));
    }
}
