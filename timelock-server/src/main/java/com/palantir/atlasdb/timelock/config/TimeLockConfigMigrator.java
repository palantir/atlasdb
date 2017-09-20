/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.config;

import java.util.Optional;

import com.palantir.atlasdb.spi.KeyValueServiceConfig;
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

    public static CombinedTimeLockServerConfiguration convert(
            TimeLockServerConfiguration config,
            Environment environment) {
        PaxosConfiguration paxosConfiguration = getPaxosConfiguration(config);
        Optional<KeyValueServiceConfig> optionalKvsConfig = getOptionalKvsConfig(config);
        TimeLockInstallConfiguration install = ImmutableTimeLockInstallConfiguration.builder()
                .optionalKvsConfig(optionalKvsConfig)
                .paxos(ImmutablePaxosInstallConfiguration.builder()
                        .dataDirectory(paxosConfiguration.paxosDataDir())
                        .build())
                .cluster(ImmutableClusterConfiguration.builder()
                        .cluster(ServiceConfiguration.builder()
                                .security(paxosConfiguration.sslConfiguration())
                                .uris(config.cluster().servers())
                                .build())
                        .localServer(config.cluster().localServer())
                        .build())
                .asyncLock(config.asyncLockConfiguration())
                .build();

        TimeLockRuntimeConfiguration runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                .paxos(ImmutablePaxosRuntimeConfiguration.builder()
                        .leaderPingResponseWaitMs(paxosConfiguration.leaderPingResponseWaitMs())
                        .maximumWaitBeforeProposalMs(paxosConfiguration.maximumWaitBeforeProposalMs())
                        .pingRateMs(paxosConfiguration.pingRateMs())
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

    private static Optional<KeyValueServiceConfig> getOptionalKvsConfig(TimeLockServerConfiguration config) {
        if (TimestampBoundStoreConfiguration.class.isInstance(config.algorithm())) {
            return Optional.of(((TimestampBoundStoreConfiguration) config.algorithm()).kvsConfig());
        }
        return Optional.empty();
    }

    private static PaxosConfiguration getPaxosConfiguration(TimeLockServerConfiguration config) {
        if (PaxosConfiguration.class.isInstance(config.algorithm())) {
            return  (PaxosConfiguration) config.algorithm();
        } else if (TimestampBoundStoreConfiguration.class.isInstance(config.algorithm())) {
            return  ((TimestampBoundStoreConfiguration) config.algorithm()).paxos();
        } else {
            throw new RuntimeException("Timelock algorithm config is of unknown type.");
        }
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
