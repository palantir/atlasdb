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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TransactionSchemaManager {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionSchemaManager.class);

    private final CoordinationService<InternalSchemaMetadata> coordinationService;
    private final CoalescingSupplier<List<ValueAndBound<InternalSchemaMetadata>>> boundPerpetuator;

    public TransactionSchemaManager(CoordinationService<InternalSchemaMetadata> coordinationService) {
        this.coordinationService = coordinationService;
        this.boundPerpetuator =
                new CoalescingSupplier<>(() -> tryPerpetuateExistingState().existingValues());
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
        if (timestamp < AtlasDbConstants.STARTING_TS) {
            throw new SafeIllegalStateException(
                    "Query attempted for timestamp {} which was never given out by the"
                            + " timestamp service, as timestamps start at {}",
                    SafeArg.of("queriedTimestamp", timestamp),
                    SafeArg.of("startOfTime", AtlasDbConstants.STARTING_TS));
        }
        Optional<Integer> possibleVersion =
                extractTimestampVersion(coordinationService.getValueForTimestamp(timestamp), timestamp);
        while (!possibleVersion.isPresent()) {
            List<ValueAndBound<InternalSchemaMetadata>> existingValues = boundPerpetuator.get();
            possibleVersion = extractTimestampVersion(
                    existingValues.stream()
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
        CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> transformResult = tryInstallNewVersion(newVersion);

        Map.Entry<Range<Long>, Integer> finalVersion =
                getRangeAtBoundThreshold(Iterables.getOnlyElement(transformResult.existingValues()));
        long finalVersionTimestampThreshold = finalVersion.getKey().lowerEndpoint();

        if (transformResult.successful() && finalVersion.getValue() == newVersion) {
            log.debug(
                    "We attempted to install the transactions schema version {}, and this was successful."
                            + " This version will take effect no later than timestamp {}.",
                    SafeArg.of("newVersion", newVersion),
                    SafeArg.of("timestamp", finalVersionTimestampThreshold));
            return true;
        }
        if (finalVersion.getValue() == newVersion) {
            log.info(
                    "We attempted to install the the transactions schema version {}. We failed, but this version"
                            + " will eventually be utilised anyway, taking effect no later than timestamp {}.",
                    SafeArg.of("newVersion", newVersion),
                    SafeArg.of("timestamp", finalVersionTimestampThreshold));
            return true;
        }
        if (transformResult.successful()) {
            log.info(
                    "We attempted to install the transactions schema version {}, but ended up installing {}"
                            + " because no version existed.",
                    SafeArg.of("versionWeTriedToInstall", newVersion),
                    SafeArg.of("versionWeActuallyInstalled", finalVersion.getValue()));
        }
        log.info(
                "We attempted to install the transactions schema version {}, but failed."
                        + " Currently, version {} will eventually be used from timestamp {}.",
                SafeArg.of("versionWeTriedToInstall", newVersion),
                SafeArg.of("versionThatWillBeUsed", finalVersion.getValue()),
                SafeArg.of("timestamp", finalVersionTimestampThreshold));
        return false;
    }

    @VisibleForTesting
    Map.Entry<Range<Long>, Integer> getRangeAtBoundThreshold(ValueAndBound<InternalSchemaMetadata> valueAndBound) {
        return valueAndBound
                .value()
                .orElseThrow(() -> new SafeIllegalStateException("Unexpectedly found no value in store"))
                .timestampToTransactionsTableSchemaVersion()
                .rangeMapView()
                .getEntry(valueAndBound.bound());
    }

    private CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> tryInstallNewVersion(int newVersion) {
        return coordinationService.tryTransformCurrentValue(
                valueAndBound -> installNewVersionInMapOrDefault(newVersion, valueAndBound));
    }

    private InternalSchemaMetadata installNewVersionInMapOrDefault(
            int newVersion, ValueAndBound<InternalSchemaMetadata> valueAndBound) {
        if (!valueAndBound.value().isPresent()) {
            log.warn(
                    "Attempting to install a new transactions schema version {}, but no past data was found,"
                            + " so we attempt to install default instead. This should normally only happen once per"
                            + " server, and only on or around first startup since upgrading to a version of AtlasDB"
                            + " that is aware of the transactions table. If this message persists, please contact"
                            + " support.",
                    SafeArg.of("newVersion", newVersion));
            return InternalSchemaMetadata.defaultValue();
        }

        log.debug(
                "Attempting to install a new transactions schema version {}, on top of schema metadata"
                        + " that is valid up till timestamp {}.",
                SafeArg.of("newVersion", newVersion),
                SafeArg.of("oldDataValidity", valueAndBound.bound()));
        InternalSchemaMetadata internalSchemaMetadata = valueAndBound.value().get();
        return InternalSchemaMetadata.builder()
                .from(internalSchemaMetadata)
                .timestampToTransactionsTableSchemaVersion(installNewVersionInMap(
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
        return coordinationService.tryTransformCurrentValue(
                valueAndBound -> valueAndBound.value().orElseGet(InternalSchemaMetadata::defaultValue));
    }

    private static Optional<Integer> extractTimestampVersion(
            Optional<ValueAndBound<InternalSchemaMetadata>> valueAndBound, long timestamp) {
        return valueAndBound
                .flatMap(ValueAndBound::value)
                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                .map(versionMap -> versionMap.getValueForTimestamp(timestamp));
    }

    @VisibleForTesting
    CoordinationService<InternalSchemaMetadata> getCoordinationService() {
        return coordinationService;
    }
}
