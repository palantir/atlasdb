/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementServiceEndpoints;
import com.palantir.atlasdb.timelock.api.management.UndertowTimeLockManagementService;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class TimeLockManagementResource implements UndertowTimeLockManagementService {
    private static final SafeLogger log = SafeLoggerFactory.get(TimeLockManagementResource.class);

    private final Set<PersistentNamespaceLoader> namespaceLoaders;
    private final TimelockNamespaces timelockNamespaces;
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final ServiceLifecycleController serviceLifecycleController;

    private TimeLockManagementResource(
            Set<PersistentNamespaceLoader> namespaceLoaders,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter,
            ServiceLifecycleController serviceLifecycleController) {
        this.namespaceLoaders = namespaceLoaders;
        this.timelockNamespaces = timelockNamespaces;
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.serviceLifecycleController = serviceLifecycleController;
    }

    public static TimeLockManagementResource create(
            PersistentNamespaceContext persistentNamespaceContext,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter,
            ServiceLifecycleController serviceLifecycleController) {
        return new TimeLockManagementResource(
                createNamespaceLoaders(persistentNamespaceContext),
                timelockNamespaces,
                redirectRetryTargeter,
                serviceLifecycleController);
    }

    public static UndertowService undertow(
            PersistentNamespaceContext persistentNamespaceContext,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter,
            ServiceLifecycleController serviceLifecycleController) {
        return TimeLockManagementServiceEndpoints.of(TimeLockManagementResource.create(
                persistentNamespaceContext, timelockNamespaces, redirectRetryTargeter, serviceLifecycleController));
    }

    public static TimeLockManagementService jersey(
            PersistentNamespaceContext persistentNamespaceContext,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter,
            ServiceLifecycleController serviceLifecycleController) {
        return new JerseyAdapter(TimeLockManagementResource.create(
                persistentNamespaceContext, timelockNamespaces, redirectRetryTargeter, serviceLifecycleController));
    }

    @Override
    public ListenableFuture<Set<String>> getNamespaces(AuthHeader authHeader) {
        // This endpoint is not used frequently (only called by migration cli), so it's okay to NOT make it async.
        return Futures.immediateFuture(namespaceLoaders.stream()
                .map(PersistentNamespaceLoader::getAllPersistedNamespaces)
                .flatMap(Set::stream)
                .filter(namespace -> !namespace.value().equals(PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE))
                .map(Client::value)
                .collect(Collectors.toSet()));
    }

    @Override
    public ListenableFuture<Void> achieveConsensus(AuthHeader authHeader, Set<String> namespaces) {
        return handleExceptions(() -> {
            for (String namespace : namespaces) {
                NamespacedConsensus.achieveConsensusForNamespace(timelockNamespaces, namespace);
            }
            return Futures.immediateFuture(null);
        });
    }

    @Override
    public ListenableFuture<Void> invalidateResources(AuthHeader authHeader, Set<String> namespaces) {
        return handleExceptions(() -> {
            namespaces.forEach(timelockNamespaces::invalidateResourcesForClient);
            return Futures.immediateFuture(null);
        });
    }

    @Override
    public ListenableFuture<UUID> getServerId(AuthHeader authHeader) {
        return Futures.immediateFuture(serviceLifecycleController.getServerId());
    }

    @Override
    public ListenableFuture<UUID> forceKillTimeLockServer(AuthHeader authHeader) {
        log.info("Forcefully stopping TimeLock service.");
        serviceLifecycleController.forceKillTimeLock();
        return Futures.immediateFuture(serviceLifecycleController.getServerId());
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    private static Set<PersistentNamespaceLoader> createNamespaceLoaders(
            PersistentNamespaceContext persistentNamespaceContext) {
        return PersistentNamespaceContexts.caseOf(persistentNamespaceContext)
                .timestampBoundPaxos((fileDataDirectory, sqliteDataSource) -> {
                    PersistentNamespaceLoader diskLoader = new DiskNamespaceLoader(fileDataDirectory);
                    PersistentNamespaceLoader sqliteLoader = SqliteNamespaceLoader.create(sqliteDataSource);
                    return ImmutableSet.of(diskLoader, sqliteLoader);
                })
                .dbBound(seriesProvider -> ImmutableSet.of(() -> seriesProvider.getKnownSeries().stream()
                        .map(TimestampSeries::series)
                        .map(Client::of)
                        .collect(Collectors.toSet())));
    }

    public static final class JerseyAdapter implements TimeLockManagementService {
        private final TimeLockManagementResource resource;

        private JerseyAdapter(TimeLockManagementResource resource) {
            this.resource = resource;
        }

        @Override
        public Set<String> getNamespaces(AuthHeader authHeader) {
            return unwrap(resource.getNamespaces(authHeader));
        }

        @Override
        public void achieveConsensus(AuthHeader authHeader, Set<String> namespaces) {
            unwrap(resource.achieveConsensus(authHeader, namespaces));
        }

        @Override
        public void invalidateResources(AuthHeader authHeader, Set<String> namespaces) {
            unwrap(resource.invalidateResources(authHeader, namespaces));
        }

        @Override
        public UUID getServerId(AuthHeader authHeader) {
            return unwrap(resource.getServerId(authHeader));
        }

        @Override
        public UUID forceKillTimeLockServer(AuthHeader authHeader) {
            return unwrap(resource.forceKillTimeLockServer(authHeader));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
