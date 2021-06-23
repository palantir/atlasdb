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
import com.palantir.atlasdb.timelock.lock.watch.LockWatchTestRuntimeConfig;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Dynamic (live-reloaded) portions of TimeLock's configuration.
 */
@JsonDeserialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockRuntimeConfiguration.class)
@Value.Immutable
@JsonIgnoreProperties("targeted-sweep-locks")
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

    @JsonProperty("test-only-lock-watches")
    @Value.Default
    public LockWatchTestRuntimeConfig lockWatchTestConfig() {
        return LockWatchTestRuntimeConfig.defaultConfig();
    }

    @JsonProperty("adjudication")
    @Value.Default
    public TimeLockAdjudicationConfiguration adjudication() {
        return ImmutableTimeLockAdjudicationConfiguration.builder().build();
    }

    @JsonProperty
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
