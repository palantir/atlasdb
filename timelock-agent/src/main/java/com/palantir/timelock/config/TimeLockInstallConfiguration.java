/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timelock.config.AsyncLockConfiguration;
import com.palantir.atlasdb.timelock.config.ImmutableAsyncLockConfiguration;

/**
 * Static (not live-reloaded) portions of TimeLock's configuration.
 */
@JsonDeserialize(as = ImmutableTimeLockInstallConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockInstallConfiguration.class)
@Value.Immutable
public interface TimeLockInstallConfiguration {
    PaxosInstallConfiguration paxos();

    @JsonProperty("key-value-service")
    Optional<KeyValueServiceConfig> optionalKvsConfig();

    ClusterConfiguration cluster();

    @Value.Default
    default AsyncLockConfiguration asyncLock() {
        return ImmutableAsyncLockConfiguration.builder().build();
    }
}
