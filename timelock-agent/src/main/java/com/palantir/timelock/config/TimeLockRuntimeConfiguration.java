/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timelock.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.logsafe.DoNotLog;
import com.palantir.tokens.auth.BearerToken;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Dynamic (live-reloaded) portions of TimeLock's configuration.
 *
 * Note that the {@link ClusterConfiguration} is an exception to above rule.
 */
@DoNotLog
@JsonDeserialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@Value.Immutable
@JsonIgnoreProperties({"targeted-sweep-locks", "test-only-lock-watches"})
public abstract class TimeLockRuntimeConfiguration {

    @Value.Default
    public PaxosRuntimeConfiguration paxos() {
        return ImmutablePaxosRuntimeConfiguration.builder().build();
    }

    /**
     *  The token required by other services to use backup and restore-related endpoints.
     */
    @JsonProperty("permitted-backup-token")
    public abstract Optional<BearerToken> permittedBackupToken();

    /**
     * As of now, TimeLock is not equipped to handle live-changes in the cluster configuration. For this reason,
     * TimeLock must be initialized with a snapshot of the clusterConfiguration which is not live-reloaded instead of
     * accessing ClusterConfiguration via {@code TimeLockRuntimeConfiguration.cluster()}.
     * */
    @JsonProperty("cluster-config-not-live-reloaded")
    public abstract ClusterConfiguration clusterSnapshot();

    /**
     * The maximum number of client namespaces to allow. Each distinct client consumes some amount of memory and disk
     * space.
     */
    @JsonProperty("max-number-of-clients")
    @Value.Default
    public Integer maxNumberOfClients() {
        return 500;
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

    @JsonProperty("adjudication")
    @Value.Default
    public TimeLockAdjudicationConfiguration adjudication() {
        return ImmutableTimeLockAdjudicationConfiguration.builder().build();
    }

    @JsonProperty("timestamp-bound-persistence")
    public abstract Optional<TsBoundPersisterRuntimeConfiguration> timestampBoundPersistence();

    @Value.Check
    public void check() {
        Preconditions.checkState(
                maxNumberOfClients() >= 0,
                "Maximum number of clients must be non-negative, but found %s",
                maxNumberOfClients());
        Preconditions.checkState(
                slowLockLogTriggerMillis() >= 0,
                "Slow lock log trigger threshold must be non-negative, but found %s",
                slowLockLogTriggerMillis());
    }
}
