/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.factory;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public final class TransactionManagers {

    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);

    private TransactionManagers() {
        // Utility class
    }

    /**
     * Accepts a single {@link Schema}.
     * @see TransactionManagers#createInMemory(Set)
     */
    public static SerializableTransactionManager createInMemory(Schema schema) {
        return createInMemory(ImmutableSet.of(schema));
    }

    /**
     * Create a {@link SerializableTransactionManager} backed by an
     * {@link com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService}. This should be used for testing
     * purposes only.
     */
    public static SerializableTransactionManager createInMemory(Set<Schema> schemas) {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder().keyValueService(new InMemoryAtlasDbConfig()).build();
        return new TransactionManagerBuilder()
                .config(config)
                .runtimeConfig(java.util.Optional::empty)
                .schemas(schemas)
                .environment(x -> { })
                .lockServerOptions(LockServerOptions.DEFAULT)
                .withHiddenTableAccess(false)
                .build();
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configurations, {@link Schema},
     * and an environment in which to register HTTP server endpoints.
     *
     * @deprecated use TransactionManagerBuilder instead.
     */
    @Deprecated
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Schema schema,
            TransactionManagerBuilder.Environment env,
            boolean allowHiddenTableAccess) {
        return new TransactionManagerBuilder()
                .config(config)
                .runtimeConfig(runtimeConfigSupplier)
                .schemas(ImmutableSet.of(schema))
                .environment(env)
                .lockServerOptions(LockServerOptions.DEFAULT)
                .withHiddenTableAccess(allowHiddenTableAccess)
                .build();
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configurations, a set of
     * {@link Schema}s, and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            TransactionManagerBuilder.Environment env,
            boolean allowHiddenTableAccess) {
        return new TransactionManagerBuilder()
                .config(config)
                .runtimeConfig(runtimeConfigSupplier)
                .schemas(schemas)
                .environment(env)
                .lockServerOptions(LockServerOptions.DEFAULT)
                .withHiddenTableAccess(allowHiddenTableAccess)
                .build();
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configurations, a set of
     * {@link Schema}s, {@link LockServerOptions}, and an environment in which to register HTTP server endpoints.

     * @deprecated use TransactionManagerBuilder instead.
     */
    @Deprecated
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            TransactionManagerBuilder.Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess) {
        return new TransactionManagerBuilder()
                .config(config)
                .runtimeConfig(runtimeConfigSupplier)
                .schemas(schemas)
                .environment(env)
                .lockServerOptions(lockServerOptions)
                .withHiddenTableAccess(allowHiddenTableAccess)
                .build();
    }

    /**
     * @deprecated use TransactionManagerBuilder instead.
     */
    @Deprecated
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            TransactionManagerBuilder.Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess,
            Class<?> callingClass) {
        return new TransactionManagerBuilder()
                .config(config)
                .runtimeConfig(runtimeConfigSupplier)
                .schemas(schemas)
                .environment(env)
                .lockServerOptions(lockServerOptions)
                .withHiddenTableAccess(allowHiddenTableAccess)
                .userAgent(UserAgents.fromClass(callingClass))
                .build();
    }

    /**
     * @deprecated use TransactionManagerBuilder instead.
     */
    @Deprecated
    public static SerializableTransactionManager create(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> optionalRuntimeConfigSupplier,
            Set<Schema> schemas,
            TransactionManagerBuilder.Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess,
            String userAgent) {
        return new TransactionManagerBuilder()
                .config(config)
                .runtimeConfig(optionalRuntimeConfigSupplier)
                .schemas(schemas)
                .environment(env)
                .lockServerOptions(lockServerOptions)
                .withHiddenTableAccess(allowHiddenTableAccess)
                .userAgent(userAgent)
                .build();
    }

    /**
     * This method should not be used directly. It remains here to support the AtlasDB-Dagger module and the CLIs, but
     * may be removed at some point in the future.
     *
     * @deprecated Not intended for public use outside of the AtlasDB CLIs
     */
    @Deprecated
    public static LockAndTimestampServices createLockAndTimestampServices(
            AtlasDbConfig config,
            TransactionManagerBuilder.Environment env,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        LockAndTimestampServices lockAndTimestampServices =
                TransactionManagerBuilder.createRawServices(config,
                        env,
                        lock,
                        time,
                        () -> {
                            log.warn("Note: Automatic migration isn't performed by the CLI tools.");
                            return AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
                        },
                        UserAgents.DEFAULT_USER_AGENT);
        return TransactionManagerBuilder.withRefreshingLockService(lockAndTimestampServices);
    }

}
