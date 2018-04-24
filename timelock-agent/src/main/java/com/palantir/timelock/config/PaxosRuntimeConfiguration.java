/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@JsonDeserialize(as = ImmutablePaxosRuntimeConfiguration.class)
@JsonSerialize(as = ImmutablePaxosRuntimeConfiguration.class)
@Value.Immutable
public interface PaxosRuntimeConfiguration {
    @JsonProperty("ping-rate-in-ms")
    @Value.Default
    default long pingRateMs() {
        return 5000L;
    }

    @JsonProperty("maximum-wait-before-proposal-in-ms")
    @Value.Default
    default long maximumWaitBeforeProposalMs() {
        return 1000L;
    }

    @JsonProperty("leader-ping-response-wait-in-ms")
    @Value.Default
    default long leaderPingResponseWaitMs() {
        return 5000L;
    }

    @JsonProperty("only-log-on-quorum-failure")
    @Value.Default
    default boolean onlyLogOnQuorumFailure() {
        return true;
    }

    @Value.Check
    default void check() {
        Preconditions.checkArgument(pingRateMs() > 0,
                "Ping rate must be positive; found '%s'.", pingRateMs());
        Preconditions.checkArgument(maximumWaitBeforeProposalMs() > 0,
                "Maximum wait before proposal must be positive; found '%s'.", maximumWaitBeforeProposalMs());
        Preconditions.checkArgument(leaderPingResponseWaitMs() > 0,
                "Leader ping response wait interval must be positive; found '%s'.", leaderPingResponseWaitMs());
    }
}
