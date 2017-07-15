/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Items in {@link TimeLockServerConfiguration} that are in neither {@link TimeLockInstallConfiguration} nor
 * {@link TimeLockRuntimeConfiguration}, but are still required at the moment.
 */
@JsonDeserialize(as = ImmutableTimeLockDeprecatedConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockDeprecatedConfiguration.class)
@Value.Immutable
public abstract class TimeLockDeprecatedConfiguration {
    @Value.Default
    public boolean useClientRequestLimit() {
        return false;
    }

    @Value.Default
    public int availableThreads() {
        return 1024;
    }

    @Value.Default
    public boolean useLockTimeLimiter() {
        return false;
    }

    @Value.Default
    public long blockingTimeoutInMs() {
        return Long.MAX_VALUE;
    }
}
