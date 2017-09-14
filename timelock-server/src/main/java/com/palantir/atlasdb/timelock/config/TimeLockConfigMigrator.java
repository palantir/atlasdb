/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.config;

import com.palantir.atlasdb.timelock.lock.BlockingTimeouts;
import com.palantir.remoting2.config.service.ServiceConfiguration;
import com.palantir.timelock.config.ImmutableClusterConfiguration;
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

        TimeLockInstallConfiguration install;
        TimeLockRuntimeConfiguration runtime;
        if (PaxosConfiguration.class.isInstance(config.algorithm())) {
            PaxosConfiguration paxos = (PaxosConfiguration) config.algorithm();
            install = ImmutableTimeLockInstallConfiguration.builder()
                    .optionalPaxosConfig(ImmutablePaxosInstallConfiguration.builder()
                            .dataDirectory(paxos.paxosDataDir())
                            .build())
                    .cluster(ImmutableClusterConfiguration.builder()
                            .cluster(ServiceConfiguration.builder()
                                    .security(paxos.sslConfiguration())
                                    .uris(config.cluster().servers())
                                    .build())
                            .localServer(config.cluster().localServer())
                            .build())
                    .asyncLock(config.asyncLockConfiguration())
                    .build();

            runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                    .optionalPaxos(ImmutablePaxosRuntimeConfiguration.builder()
                            .leaderPingResponseWaitMs(paxos.leaderPingResponseWaitMs())
                            .maximumWaitBeforeProposalMs(paxos.maximumWaitBeforeProposalMs())
                            .pingRateMs(paxos.pingRateMs())
                            .build())
                    .slowLockLogTriggerMillis(config.slowLockLogTriggerMillis())
                    .build();
        } else if (TimestampBoundStoreConfiguration.class.isInstance(config.algorithm())) {
            TimestampBoundStoreConfiguration timestampBoundStoreConfiguration =
                    (TimestampBoundStoreConfiguration) config.algorithm();
            install = ImmutableTimeLockInstallConfiguration.builder()
                    .cluster(ImmutableClusterConfiguration.builder()
                            .cluster(ServiceConfiguration.builder()
                                    .security(timestampBoundStoreConfiguration.sslConfiguration())
                                    .uris(config.cluster().servers())
                                    .build())
                            .localServer(config.cluster().localServer())
                            .build())
                    .asyncLock(config.asyncLockConfiguration())
                    .build();

            runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                    .slowLockLogTriggerMillis(config.slowLockLogTriggerMillis())
                    .build();
        } else {
            throw new RuntimeException("The algorithm config was expected to be of type PaxosConfiguration "
                    + "or TimestampBoundStoreConfiguration, but found " + config.algorithm().getClass());
        }

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
