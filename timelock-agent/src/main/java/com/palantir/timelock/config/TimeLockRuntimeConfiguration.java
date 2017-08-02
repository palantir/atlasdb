/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

/**
 * Dynamic (live-reloaded) portions of TimeLock's configuration.
 */
@JsonDeserialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@Value.Immutable
public abstract class TimeLockRuntimeConfiguration {
    private static final String CLIENT_NAME_REGEX = "[a-zA-Z0-9_-]+";

    public abstract Optional<PaxosRuntimeConfiguration> algorithm();

    public abstract Set<String> clients();

    /**
     * Log at INFO if a lock request receives a response after given duration in milliseconds.
     * Default value is 10000 millis or 10 seconds.
     */
    @JsonProperty("slow-lock-log-trigger-in-ms")
    @Value.Default
    public long slowLockLogTriggerMillis() {
        return 10000;
    }

    @JsonProperty("partitioner")
    @Value.Default
    public PartitionerConfiguration partitioner() {
        return ImmutableLptPartitionerConfiguration.builder()
                .miniclusterSize(2)
                .build();
    }

    @Value.Check
    public void check() {
        Preconditions.checkState(slowLockLogTriggerMillis() >= 0,
                "Slow lock log trigger threshold must be nonnegative, but found %s", slowLockLogTriggerMillis());
        clients().forEach(client -> Preconditions.checkState(
                client.matches(CLIENT_NAME_REGEX),
                "Client names must consist of alphanumeric characters, underscores, or dashes. Illegal name: %s",
                client));
        Preconditions.checkState(!clients().contains(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE),
                "The client name '%s' is reserved for the leader election service, and may not be used.",
                PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE);
    }
}
