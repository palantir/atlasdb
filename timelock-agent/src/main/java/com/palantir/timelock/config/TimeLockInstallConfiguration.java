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
import com.palantir.atlasdb.debug.LockDiagnosticConfig;
import com.palantir.atlasdb.timelock.config.TimeLockConstants;
import com.palantir.paxos.Client;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Static (not live-reloaded) portions of TimeLock's configuration.
 */
@JsonDeserialize(as = ImmutableTimeLockInstallConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockInstallConfiguration.class)
@Value.Immutable
@JsonIgnoreProperties(value = "asyncLock")
public interface TimeLockInstallConfiguration {
    PaxosInstallConfiguration paxos();

    /**
     * @deprecated The ClusterInstallConfiguration has been moved to
     * {@link TimeLockRuntimeConfiguration#clusterSnapshot()}.
     */
    @Deprecated
    @Value.Default
    default ClusterInstallConfiguration cluster() {
        return ImmutableClusterInstallConfiguration.builder().build();
    }

    @Value.Default
    default boolean iAmOnThePersistenceTeamAndKnowWhatImDoingSkipSqliteConsistencyCheckAndTruncateFileBasedLog() {
        return false;
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @JsonProperty("lock-diagnostic-config")
    Map<Client, LockDiagnosticConfig> lockDiagnosticConfig();

    @Value.Default
    default TsBoundPersisterConfiguration timestampBoundPersistence() {
        return ImmutablePaxosTsBoundPersisterConfiguration.builder().build();
    }

    @Value.Default
    default boolean iAmOnThePersistenceTeamAndKnowWhatIAmDoingReseedPersistedPersisterConfiguration() {
        return false;
    }

    @Value.Derived
    default boolean isNewService() {
        return paxos().isNewService();
    }

    /**
     * TODO(gs): Remove this once PDS-264792 is resolved
     *
     * Defines the probability at which, upon receiving a request whilst not being the leader, we
     * refuse to follow our own leader hint, and return QoS.unavailable().
     *
     * This gives us a 95% chance of hitting the correct node in the usual case, and a 5% chance of
     * escaping from an infinite loop of 308s in the failure case (where our hint is wrong).
     *
     * We may want to reduce this if too many redirects make this slow.
     */
    @Value.Default
    default double randomRedirectProbability() {
        return TimeLockConstants.DEFAULT_RANDOM_REDIRECT_PROBABILITY;
    }

    static Builder builder() {
        return new Builder();
    }

    class Builder extends ImmutableTimeLockInstallConfiguration.Builder {}
}
