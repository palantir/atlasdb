/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.config;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.lock.BlockingTimeouts;
import com.palantir.remoting.api.config.service.PartialServiceConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosInstallConfiguration;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockInstallConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
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
                        .build())
                .cluster(ImmutableDefaultClusterConfiguration.builder()
                        .cluster(PartialServiceConfiguration.builder()
                                .security(paxos.sslConfiguration())
                                .uris(config.cluster().servers())
                                .build())
                        .localServer(config.cluster().localServer())
                        .build())
                .asyncLock(config.asyncLockConfiguration())
                .build();

        TimeLockRuntimeConfiguration runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                .paxos(ImmutablePaxosRuntimeConfiguration.builder()
                        .leaderPingResponseWaitMs(paxos.leaderPingResponseWaitMs())
                        .maximumWaitBeforeProposalMs(paxos.maximumWaitBeforeProposalMs())
                        .pingRateMs(paxos.pingRateMs())
                        .build())
                .slowLockLogTriggerMillis(config.slowLockLogTriggerMillis())
                .build();

        TimeLockDeprecatedConfiguration deprecated = createDeprecatedConfiguration(config, environment);

        return ImmutableCombinedTimeLockServerConfiguration.builder()
                .install(install)
                .runtime(runtime)
                .deprecated(deprecated)
                .build();
    }

    private static TimeLockDeprecatedConfiguration createDeprecatedConfiguration(TimeLockServerConfiguration config,
            Environment environment) {
        ImmutableTimeLockDeprecatedConfiguration.Builder deprecatedBuilder
                = ImmutableTimeLockDeprecatedConfiguration.builder();

        if (config.timeLimiterConfiguration().enableTimeLimiting()) {
            deprecatedBuilder.useLockTimeLimiter(true);
            deprecatedBuilder.blockingTimeoutInMs(
                    BlockingTimeouts.getBlockingTimeout(environment.getObjectMapper(), config));
        }
        if (config.useClientRequestLimit()) {
            deprecatedBuilder.useClientRequestLimit(true);
            deprecatedBuilder.availableThreads(config.availableThreads());
        }

        return deprecatedBuilder.build();
    }
}
