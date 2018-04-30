/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

@JsonDeserialize(as = ImmutableCombinedTimeLockServerConfiguration.class)
@JsonSerialize(as = ImmutableCombinedTimeLockServerConfiguration.class)
@Value.Immutable
public interface CombinedTimeLockServerConfiguration {
    TimeLockInstallConfiguration install();

    TimeLockRuntimeConfiguration runtime();

    TimeLockDeprecatedConfiguration deprecated();
}
