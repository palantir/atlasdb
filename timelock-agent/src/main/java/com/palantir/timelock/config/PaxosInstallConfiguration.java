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

import java.io.File;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

@JsonDeserialize(as = ImmutablePaxosInstallConfiguration.class)
@JsonSerialize(as = ImmutablePaxosInstallConfiguration.class)
@Value.Immutable
public interface PaxosInstallConfiguration {
    @JsonProperty("data-directory")
    @Value.Default
    default File dataDirectory() {
        // TODO (jkong): should this value be something like "mnt/timelock/paxos" given we'll be
        // given a persisted volume that will be mounted inside $SERVICE_HOME in k8s/docker
        // TODO (jkong): should we just have a generic "dataDirectory" field at root TimeLockInstallConfiguration
        // level and delete this entire file?
        return new File("var/data/paxos");
    }

    /**
     * Set to true if this is a new stack. Otherwise, set to false.
     */
    @JsonProperty("is-new-service")
    boolean isNewService();

    enum PaxosLeaderMode {
        SINGLE_LEADER,
        LEADER_PER_CLIENT,
        AUTO_MIGRATION_MODE
    }

    @JsonProperty("leader-mode")
    @Value.Default
    default PaxosLeaderMode leaderMode() {
        return PaxosLeaderMode.SINGLE_LEADER;
    }

    @Value.Check
    default void checkLeaderModeIsNotInAutoMigrationMode() {
        Preconditions.checkState(
                leaderMode() != PaxosLeaderMode.AUTO_MIGRATION_MODE,
                "Auto migration mode is not supported just yet");
    }

    @Value.Check
    default void check() {
        if (isNewService() && dataDirectory().isDirectory()) {
            throw new SafeIllegalArgumentException(
                    "This timelock server has been configured as a new stack (the 'is-new-service' property is set to "
                            + "true), but the Paxos data directory already exists. Almost surely this is because it "
                            + "has already been turned on at least once, and thus the 'is-new-service' property should "
                            + "be set to false for safety reasons.");
        }

        if (!isNewService() && !dataDirectory().isDirectory()) {
            throw new SafeIllegalArgumentException("The timelock data directory does not appear to exist. If you are "
                    + "trying to move the nodes on your timelock cluster or add new nodes, you have likely already "
                    + "made a mistake by this point. This is a non-trivial operation and risks service corruption, "
                    + "so contact support for assistance. Otherwise, if this is a new timelock service, please "
                    + "configure paxos.is-new-service to true for the first startup only of each node.");
        }
    }

}
