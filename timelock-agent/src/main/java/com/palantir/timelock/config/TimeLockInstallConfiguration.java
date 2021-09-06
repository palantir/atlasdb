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

    @Deprecated
    ClusterInstallConfiguration cluster();

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

    static Builder builder() {
        return new Builder();
    }

    class Builder extends ImmutableTimeLockInstallConfiguration.Builder {}
}
