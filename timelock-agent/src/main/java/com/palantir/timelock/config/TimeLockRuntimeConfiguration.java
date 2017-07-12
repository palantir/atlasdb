/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

/**
 * Dynamic (live-reloaded) portions of TimeLock's configuration.
 */
public abstract class TimeLockRuntimeConfiguration {
    private static final String CLIENT_NAME_REGEX = "[a-zA-Z0-9_-]+";

    abstract Optional<TimeLockAlgorithmRuntimeConfiguration> algorithm();
//
//    Per JKong: "TimeLimiter might not be needed"
//    /**
//     * Returns a value indicating the margin of error we leave before interrupting a long running request,
//     * since we wish to perform this interruption and return a BlockingTimeoutException _before_ Jetty closes the
//     * stream. This margin is specified as a ratio of the smallest idle timeout - hence it must be in (0, 1).
//     */
//    @JsonProperty("blocking-timeout-error-margin")
//    @Value.Default
//    Optional<Double> blockingTimeoutErrorMargin() {
//        return Optional.of(0.03);
//    }

    abstract Set<String> clients();

    /**
     * Log at INFO if a lock request receives a response after given duration in milliseconds.
     * Default value is 10000 millis or 10 seconds.
     */
    @JsonProperty("slow-lock-log-trigger-in-ms")
    @Value.Default
    long slowLockLogTriggerMillis() {
        return 10000;
    }
//
//    Per NZiebart: "I think we should just remove the ability to configure that going forward"
//    @JsonProperty("use-client-request-limit")
//    @Value.Default
//    boolean useClientRequestLimit() {
//        return false;
//    }

    @Value.Check
    void check() {
        clients().forEach(client -> Preconditions.checkState(
                client.matches(CLIENT_NAME_REGEX),
                "Client names must consist of alphanumeric characters, underscores, or dashes. Illegal name: %s",
                client));
        Preconditions.checkState(!clients().contains(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE),
                "The client name '%s' is reserved for the leader election service, and may not be used.",
                PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE);
//
//        Preconditions.checkState(blockingTimeoutErrorMargin() > 0 && blockingTimeoutErrorMargin() < 1,
//                "Lock service timeout margin must be strictly between 0 and 1. Illegal timeout: %s",
//                blockingTimeoutErrorMargin());
    }
}
