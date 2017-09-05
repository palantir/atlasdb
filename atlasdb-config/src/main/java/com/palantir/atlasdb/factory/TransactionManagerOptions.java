/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.factory;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.factory.TransactionManagers.Environment;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.lock.LockServerOptions;

@Value.Immutable
public interface TransactionManagerOptions {
    AtlasDbConfig config();

    @Value.Default
    default Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier() {
        return Optional::empty;
    }

    Set<Schema> schemas();

    @Value.Default
    default Environment env() {
        return resource -> { };
    }

    @Value.Default
    default LockServerOptions lockServerOptions() {
        return LockServerOptions.DEFAULT;
    }

    @Value.Default
    default boolean allowHiddenTableAccess() {
        return false;
    }

    Optional<Class<?>> callingClass();

    Optional<String> userAgent();

    // directly specified -> inferred from caller -> default
    @Value.Derived
    default String derivedUserAgent() {
        return userAgent().orElse(callingClass().map(UserAgents::fromClass).orElse(UserAgents.DEFAULT_USER_AGENT));
    }

    static Builder builder() {
        return new Builder();
    }

    class Builder extends ImmutableTransactionManagerOptions.Builder {}
}
