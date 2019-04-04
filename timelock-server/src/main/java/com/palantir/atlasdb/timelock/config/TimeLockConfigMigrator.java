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
package com.palantir.atlasdb.timelock.config;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.lock.BlockingTimeouts;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosInstallConfiguration;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockInstallConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.dropwizard.setup.Environment;

public final class TimeLockConfigMigrator {
    private TimeLockConfigMigrator() { /* Utility Class */ }

    public static CombinedTimeLockServerConfiguration convert(TimeLockServerConfiguration config,
            Environment environment) {
        // taking advantage of the fact that there is only one algorithm impl at the moment
        Preconditions.checkArgument(PaxosConfiguration.class.isInstance(config.algorithm()),
                "Paxos is the only leader election algorithm currently supported. Not: %s",
                config.algorithm().getClass());
        PaxosConfiguration paxos = (PaxosConfiguration) config.algorithm();

        TimeLockInstallConfiguration install = ImmutableTimeLockInstallConfiguration.builder()
                .timestampBoundPersistence(config.getTsBoundPersisterConfiguration())
                .paxos(ImmutablePaxosInstallConfiguration.builder()
                        .dataDirectory(paxos.paxosDataDir())
                        .isNewService(paxos.isNewService())
                        .build())
                .cluster(ImmutableDefaultClusterConfiguration.builder()
                        .cluster(PartialServiceConfiguration.builder()
                                .security(paxos.sslConfiguration())
                                .uris(config.cluster().servers())
                                .build())
                        .localServer(config.cluster().localServer())
                        .build())
                .build();

        TimeLockRuntimeConfiguration runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                .paxos(ImmutablePaxosRuntimeConfiguration.builder()
                        .leaderPingResponseWaitMs(paxos.leaderPingResponseWaitMs())
                        .maximumWaitBeforeProposalMs(paxos.maximumWaitBeforeProposalMs())
                        .pingRateMs(paxos.pingRateMs())
                        .build())
                .slowLockLogTriggerMillis(config.slowLockLogTriggerMillis())
                .build();

        return ImmutableCombinedTimeLockServerConfiguration.builder()
                .install(install)
                .runtime(runtime)
                .threadPoolSize(config.availableThreads())
                .blockingTimeoutMs(getBlockingTimeout(config, environment))
                .build();
    }

    private static long getBlockingTimeout(TimeLockServerConfiguration config, Environment environment) {
        return BlockingTimeouts.getBlockingTimeout(environment.getObjectMapper(), config);
    }
}
