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
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.lock.client.CloseableSupplier;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LazyInstanceCloseableSupplier;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.v2.TimestampLeaseResult;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.function.Supplier;

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
        // TODO(aalouane): implement
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() {
        acquirerService.close();
    }
}
