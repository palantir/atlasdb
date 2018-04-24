/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

/**
 * Items in {@link TimeLockServerConfiguration} that are in neither {@link TimeLockInstallConfiguration} nor
 * {@link TimeLockRuntimeConfiguration}, but are still required at the moment.
 */
@JsonDeserialize(as = ImmutableTimeLockDeprecatedConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockDeprecatedConfiguration.class)
@Value.Immutable
public abstract class TimeLockDeprecatedConfiguration {
    @JsonProperty("use-client-request-limit")
    @Value.Default
    public boolean useClientRequestLimit() {
        return false;
    }

    @JsonProperty("available-threads")
    @Value.Default
    public int availableThreads() {
        return 1024;
    }

    @JsonProperty("use-lock-time-limiter")
    @Value.Default
    public boolean useLockTimeLimiter() {
        return false;
    }

    @JsonProperty("blocking-timeout-in-ms")
    @Value.Default
    public long blockingTimeoutInMs() {
        return Long.MAX_VALUE;
    }

    @Value.Check
    public void check() {
        Preconditions.checkState(availableThreads() >= 0,
                "Number of available threads must be nonnegative, but found %s", availableThreads());
        Preconditions.checkState(blockingTimeoutInMs() >= 0,
                "Blocking timeout must be nonnegative, but found %s", blockingTimeoutInMs());
    }
}
