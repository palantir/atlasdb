/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Items in {@link TimeLockServerConfiguration} that are in neither {@link TimeLockInstallConfiguration} nor
 * {@link TimeLockRuntimeConfiguration}, but are still required at the moment.
 */
@JsonDeserialize(as = ImmutableTimeLockDeprecatedConfiguration.class)
@JsonSerialize(as = ImmutableTimeLockDeprecatedConfiguration.class)
@Value.Immutable
public interface TimeLockDeprecatedConfiguration {

}
