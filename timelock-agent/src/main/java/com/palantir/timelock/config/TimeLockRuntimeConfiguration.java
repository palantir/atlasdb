/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import java.util.Map;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Dynamic (live-reloaded) portions of TimeLock's configuration.
 */
@JsonDeserialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@Value.Immutable
public abstract class TimeLockRuntimeConfiguration {

    @Value.Default
    public PaxosRuntimeConfiguration paxos() {
        return ImmutablePaxosRuntimeConfiguration.builder().build();
    }

    /**
     * The maximum number of client namespaces to allow. Each distinct client consumes some amount of memory and disk
     * space.
     */
    @JsonProperty("max-number-of-clients")
    @Value.Default
    public Integer maxNumberOfClients() {
        return 100;
    }

    /**
     * Log at INFO if a lock request receives a response after given duration in milliseconds.
     * Default value is 10000 millis or 10 seconds.
     */
    @JsonProperty("slow-lock-log-trigger-in-ms")
    @Value.Default
    public long slowLockLogTriggerMillis() {
        return 10000;
    }

    @JsonProperty("client-tokens")
    @Value.Default
    public Map<String, String> clientTokens() {
        return ImmutableMap.of();
    }

    @JsonProperty("admin-token")
    public abstract String adminToken();

    @Value.Check
    public void check() {
        Preconditions.checkState(slowLockLogTriggerMillis() >= 0,
                "Slow lock log trigger threshold must be nonnegative, but found %s", slowLockLogTriggerMillis());
    }
}
