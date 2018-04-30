/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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

    ClusterConfiguration cluster();

    @Value.Default
    default TsBoundPersisterConfiguration timestampBoundPersistence() {
        return ImmutablePaxosTsBoundPersisterConfiguration.builder().build();
    }

    @Value.Default
    default AsyncLockConfiguration asyncLock() {
        return ImmutableAsyncLockConfiguration.builder().build();
    }
}
