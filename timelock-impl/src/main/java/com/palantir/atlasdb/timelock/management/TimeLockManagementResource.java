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

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import com.palantir.paxos.Client;
import com.palantir.tokens.auth.AuthHeader;

public class TimeLockManagementResource implements UndertowTimeLockManagementService {
    private final Set<PersistentNamespaceLoader> namespaceLoaders;
    private final TimelockNamespaces timelockNamespaces;
    private final ConjureResourceExceptionHandler exceptionHandler;

    private TimeLockManagementResource(Set<PersistentNamespaceLoader> namespaceLoaders,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter) {
        this.namespaceLoaders = namespaceLoaders;
        this.timelockNamespaces = timelockNamespaces;
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
    }

    public static TimeLockManagementResource create(
            PersistentNamespaceContext persistentNamespaceContext,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter) {
        return new TimeLockManagementResource(
                createNamespaceLoaders(persistentNamespaceContext), timelockNamespaces, redirectRetryTargeter);
    }

    public static UndertowService undertow(
            PersistentNamespaceContext persistentNamespaceContext,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter) {
        return TimeLockManagementServiceEndpoints.of(TimeLockManagementResource.create(
                persistentNamespaceContext, timelockNamespaces, redirectRetryTargeter));
    }

    public static TimeLockManagementService jersey(
            PersistentNamespaceContext persistentNamespaceContext,
            TimelockNamespaces timelockNamespaces,
            RedirectRetryTargeter redirectRetryTargeter) {
        return new JerseyAdapter(TimeLockManagementResource.create(
                persistentNamespaceContext, timelockNamespaces, redirectRetryTargeter));
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
                NamespacedConsensus
                        .achieveConsensusForNamespace(timelockNamespaces, namespace);
            }
            return Futures.immediateFuture(null);
        });
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
                .dbBound(seriesProvider -> ImmutableSet.of(
                        () -> seriesProvider.getKnownSeries()
                                .stream()
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

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
