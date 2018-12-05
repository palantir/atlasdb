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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class TransactionSchemaManager {
    private final CoordinationService<InternalSchemaMetadata> coordinationService;

    public TransactionSchemaManager(CoordinationService<InternalSchemaMetadata> coordinationService) {
        this.coordinationService = coordinationService;
    }

    /**
     * Returns the version of the transactions schema associated with the provided timestamp.
     *
     * This method may perpetuate the existing state one or more times to achieve consensus. It will repeatedly
     * attempt to perpetuate the existing state until a consensus for the provided timestamp argument is achieved.
     *
     * This method should only be called with timestamps that have already been given out by the timestamp service;
     * otherwise, achieving a consensus may take a long time.
     */
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

    /**
     * Attempts to install a new transactions table schema version, by submitting a relevant transform.
     *
     * The execution of this method does not guarantee that the provided version will eventually be installed.
     * This method returns true if and only if in the map agreed by the coordination service evaluated at the validity
     * bound, the transactions schema version is equal to newVersion.
     */
    public boolean tryInstallNewTransactionsSchemaVersion(int newVersion) {
        List<Integer> presentVersionPeakValidity = coordinationService.tryTransformCurrentValue(
                valueAndBound -> installNewVersionInMapIfPresent(newVersion, valueAndBound))
                .existingValues()
                .stream()
                .map(valueAndBound -> valueAndBound.value()
                        .orElseThrow(() -> new SafeIllegalStateException("Unexpectedly found no value in store"))
                        .timestampToTransactionsTableSchemaVersion()
                        .getValueForTimestamp(valueAndBound.bound()))
                .collect(Collectors.toList());
        return Iterables.getOnlyElement(presentVersionPeakValidity) == newVersion;
    }

    private InternalSchemaMetadata installNewVersionInMapIfPresent(int newVersion,
            ValueAndBound<InternalSchemaMetadata> valueAndBound) {
        if (!valueAndBound.value().isPresent()) {
            return InternalSchemaMetadata.defaultValue();
        }

        InternalSchemaMetadata internalSchemaMetadata = valueAndBound.value().get();
        return InternalSchemaMetadata.builder()
                .from(internalSchemaMetadata)
                .timestampToTransactionsTableSchemaVersion(
                        installNewVersionInMap(
                                internalSchemaMetadata.timestampToTransactionsTableSchemaVersion(),
                                valueAndBound.bound() + 1,
                                newVersion))
                .build();
    }

    private TimestampPartitioningMap<Integer> installNewVersionInMap(
            TimestampPartitioningMap<Integer> sourceMap, long bound, int newVersion) {
        return sourceMap.copyInstallingNewValue(bound, newVersion);
    }

    private CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> tryPerpetuateExistingState() {
        return coordinationService.tryTransformCurrentValue(valueAndBound ->
                valueAndBound.value().orElseGet(InternalSchemaMetadata::defaultValue));
    }

    private static Optional<Integer> extractTimestampVersion(
            Optional<ValueAndBound<InternalSchemaMetadata>> valueAndBound, long timestamp) {
        return valueAndBound
                .flatMap(ValueAndBound::value)
                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                .map(versionMap -> versionMap.getValueForTimestamp(timestamp));
    }
}
