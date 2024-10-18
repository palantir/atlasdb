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

package com.palantir.lock.client.timestampleases;

import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.client.CloseableSupplier;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LazyInstanceCloseableSupplier;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.v2.PartialTimestampLeaseResult;
import com.palantir.lock.v2.TimestampLeaseResult;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class TimestampLeaseAcquirerImpl implements TimestampLeaseAcquirer {
    private static final SafeLogger log = SafeLoggerFactory.get(TimestampLeaseAcquirerImpl.class);

    private final Namespace namespace;
    private final CloseableSupplier<MultiClientTimestampLeaseAcquirer> acquirerService;
    private final NamespacedConjureTimelockService timelockService;

    private TimestampLeaseAcquirerImpl(
            Namespace namespace,
            CloseableSupplier<MultiClientTimestampLeaseAcquirer> multiClientService,
            NamespacedConjureTimelockService timelockService) {
        this.namespace = namespace;
        this.acquirerService = multiClientService;
        this.timelockService = timelockService;
    }

    private static TimestampLeaseAcquirer create(
            Namespace namespace,
            CloseableSupplier<MultiClientTimestampLeaseAcquirer> multiClientService,
            NamespacedConjureTimelockService namespacedService) {
        return new TimestampLeaseAcquirerImpl(namespace, multiClientService, namespacedService);
    }

    public static TimestampLeaseAcquirer create(
            String namespace,
            CloseableSupplier<MultiClientTimestampLeaseAcquirer> multiClientService,
            NamespacedConjureTimelockService namespacedService) {
        return create(Namespace.of(namespace), multiClientService, namespacedService);
    }

    public static TimestampLeaseAcquirer create(
            Namespace namespace,
            Supplier<InternalMultiClientConjureTimelockService> multiClientService,
            NamespacedConjureTimelockService namespacedService) {
        return create(
                namespace,
                LazyInstanceCloseableSupplier.of(
                        Suppliers.compose(MultiClientTimestampLeaseAcquirer::create, multiClientService::get)),
                namespacedService);
    }

    public static TimestampLeaseAcquirer create(
            String namespace,
            Supplier<InternalMultiClientConjureTimelockService> multiClientService,
            NamespacedConjureTimelockService namespacedService) {
        return create(
                namespace,
                LazyInstanceCloseableSupplier.of(
                        Suppliers.compose(MultiClientTimestampLeaseAcquirer::create, multiClientService::get)),
                namespacedService);
    }

    @Override
    public Map<TimestampLeaseName, TimestampLeaseResult> acquireNamedTimestampLeases(
            Map<TimestampLeaseName, Integer> requests) {
        Map<TimestampLeaseName, PartialTimestampLeaseResult> results =
                acquirerService.getDelegate().acquireTimestampLeases(namespace, requests);
        return KeyedStream.stream(results)
                .map((timestampName, partialResult) ->
                        completeResult(timestampName, partialResult, requests.get(timestampName)))
                .collectToMap();
    }

    private TimestampLeaseResult completeResult(
            TimestampLeaseName timestampName,
            PartialTimestampLeaseResult partialResult,
            @Nullable Integer numRequested) {
        if (numRequested == null) {
            log.error(
                    "Expected for there to be a timestamp name but found none",
                    SafeArg.of("timestampName", timestampName));
            throw new SafeIllegalStateException(
                    "Expected for there to be a timestamp name but found none",
                    SafeArg.of("timestampName", timestampName));
        }

        LongSupplier timestampSupplier = createExactTimestampSupplier(partialResult, numRequested);
        return TimestampLeaseResult.fromPartialResult(partialResult, timestampSupplier);
    }

    private LongSupplier createExactTimestampSupplier(PartialTimestampLeaseResult partialResult, int numRequested) {
        if (partialResult.freshTimestamps().size() >= numRequested) {
            return ExactRangeBackedTimestampSuppliers.createFromRange(partialResult.freshTimestamps(), numRequested);
        }

        int deficit =
                Ints.checkedCast(numRequested - partialResult.freshTimestamps().size());
        List<TimestampRange> ranges = ExactTimestampRangeSupplier.getFreshTimestamps(deficit, timelockService);
        return ExactRangeBackedTimestampSuppliers.createFromRanges(
                numRequested, partialResult.freshTimestamps(), ranges);
    }

    @Override
    public void close() {
        acquirerService.close();
    }
}
