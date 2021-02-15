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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.Beta;
import com.palantir.logsafe.Preconditions;
import java.io.File;
import java.io.IOException;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePaxosInstallConfiguration.class)
@JsonSerialize(as = ImmutablePaxosInstallConfiguration.class)
@Value.Immutable
public interface PaxosInstallConfiguration {
    /**
     * Data directory used to store file-backed Paxos log state.
     */
    @JsonProperty("data-directory")
    @Value.Default
    default File dataDirectory() {
        // TODO (jkong): should this value be something like "mnt/timelock/paxos" given we'll be
        // given a persisted volume that will be mounted inside $SERVICE_HOME in k8s/docker
        // TODO (jkong): should we just have a generic "dataDirectory" field at root TimeLockInstallConfiguration
        // level and delete this entire file?
        return new File("var/data/paxos");
    }

    @Beta
    @JsonProperty("sqlite-persistence")
    @Value.Default
    default SqlitePaxosPersistenceConfiguration sqlitePersistence() {
        return SqlitePaxosPersistenceConfigurations.DEFAULT;
    }

    /**
     * Set to true if this is a new stack. Otherwise, set to false.
     */
    @JsonProperty("is-new-service")
    boolean isNewService();

    /**
     * If true, TimeLock is allowed to create clients that it has never seen before.
     * If false, then TimeLock will continue to serve requests for clients that it already knows about (across the
     * history of its persistent state, not just the life of the current JVM), but it will not accept any new clients.
     */
    @JsonProperty("can-create-new-clients")
    default boolean canCreateNewClients() {
        return true;
    }

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
                leaderMode() != PaxosLeaderMode.AUTO_MIGRATION_MODE, "Auto migration mode is not supported just yet");
    }

    @Value.Check
    default void checkSqliteAndFileDataDirectoriesAreNotPossiblyShared() {
        try {
            Preconditions.checkArgument(
                    !sqlitePersistence().dataDirectory().equals(dataDirectory()),
                    "SQLite and file-based data directories must be different!");
            Preconditions.checkArgument(
                    !sqlitePersistence()
                            .dataDirectory()
                            .getCanonicalPath()
                            .startsWith(dataDirectory().getCanonicalPath() + File.separator),
                    "SQLite data directory can't be a subdirectory of the file-based data directory!");
            Preconditions.checkArgument(
                    !dataDirectory()
                            .getCanonicalPath()
                            .startsWith(sqlitePersistence().dataDirectory().getCanonicalPath() + File.separator),
                    "File-based data directory can't be a subdirectory of the SQLite data directory!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Value.Derived
    default boolean doDataDirectoriesExist() {
        return dataDirectory().isDirectory();
    }
}
