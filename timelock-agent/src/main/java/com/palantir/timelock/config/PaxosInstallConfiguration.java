/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import java.io.File;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

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

    @Value.Check
    default void check() {
        if (isNewService() && dataDirectory().isDirectory()) {
            throw new IllegalArgumentException(
                    "This timelock server has been configured as a new stack (the 'is-new-service' property is set to "
                            + "true), but the Paxos data directory already exists. Almost surely this is because it "
                            + "has already been turned on at least once, and thus the 'is-new-service' property should "
                            + "be set to false for safety reasons.");
        }

        if (!isNewService() && !dataDirectory().isDirectory()) {
            throw new IllegalArgumentException(
                    "If this is a new timelock service, please configure paxos.is-new-service to true for the first "
                            + "startup only of each node. \n\nOtherwise, the timelock data directory appears to no "
                            + "longer exist. If you are trying to move the nodes on your timelock cluster or add new "
                            + "nodes, you have likely already made a mistake by this point. This is a non-trivial "
                            + "operation, please be careful and do not be afraid to seek out help.");
        }

        Preconditions.checkArgument(dataDirectory().mkdirs() || dataDirectory().isDirectory(),
                "Could not create paxos data directory %s", dataDirectory());
    }
}
