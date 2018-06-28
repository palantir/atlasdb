/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.paxos;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.util.AsyncOrLegacyTimelockService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.TimelockService;

public class LegacyTimeLockServicesCreator implements TimeLockServicesCreator {
    private static final Logger log = LoggerFactory.getLogger(LegacyTimeLockServicesCreator.class);
    private static final LockClient LEGACY_LOCK_CLIENT = LockClient.of("legacy");

    private final MetricRegistry metricRegistry;
    private final PaxosLeadershipCreator leadershipCreator;

    public LegacyTimeLockServicesCreator(MetricRegistry metricRegistry,
            PaxosLeadershipCreator leadershipCreator) {
        this.metricRegistry = metricRegistry;
        this.leadershipCreator = leadershipCreator;
    }

    @Override
    public TimeLockServices createTimeLockServices(
            String client,
            Supplier<ManagedTimestampService> rawTimestampServiceSupplier,
            Supplier<LockService> rawLockServiceSupplier) {
        log.info("Creating legacy timelock service for client {}", client);
        ManagedTimestampService timestampService = instrumentInLeadershipProxy(
                ManagedTimestampService.class,
                rawTimestampServiceSupplier,
                client);
        LockService lockService = instrumentInLeadershipProxy(
                LockService.class,
                rawLockServiceSupplier,
                client);

        // The underlying primitives are already wrapped in a leadership proxy (and must be).
        // Wrapping this means that we will make 2 paxos checks per request, which is silly.
        TimelockService legacyTimelockService = instrument(
                TimelockService.class,
                createRawLegacyTimelockService(timestampService, lockService),
                client);
        return TimeLockServices.create(
                timestampService,
                lockService,
                AsyncOrLegacyTimelockService.createFromLegacyTimelock(legacyTimelockService),
                timestampService);
    }

    private static LegacyTimelockService createRawLegacyTimelockService(
            ManagedTimestampService timestampService,
            LockService lockService) {
        return new LegacyTimelockService(timestampService, lockService, LEGACY_LOCK_CLIENT);
    }

    private <T> T instrumentInLeadershipProxy(Class<T> serviceClass, Supplier<T> serviceSupplier, String client) {
        return instrument(serviceClass, leadershipCreator.wrapInLeadershipProxy(serviceSupplier, serviceClass), client);
    }

    private <T> T instrument(Class<T> serviceClass, T service, String client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass));
    }
}
