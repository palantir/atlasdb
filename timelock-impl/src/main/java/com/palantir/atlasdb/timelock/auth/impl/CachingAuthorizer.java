/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.auth.impl;

import java.util.Map;

import org.immutables.value.Value;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;
import com.palantir.atlasdb.timelock.auth.api.Authorizer;
import com.palantir.atlasdb.timelock.auth.api.AuthenticatedClient;

public class CachingAuthorizer implements Authorizer {
    private final Map<ClientId, Privileges> privileges;
    private final NamespaceLocker namespaceLocker;
    private final LoadingCache<CacheKey, Boolean> cache;

    @VisibleForTesting
    CachingAuthorizer(Map<ClientId, Privileges> privileges, NamespaceLocker namespaceLocker) {
        this.privileges = privileges;
        this.namespaceLocker = namespaceLocker;
        this.cache = Caffeine.newBuilder()
                .maximumSize(1000)
                .build(key -> isAuthorizedInternal(key.authenticatedClient(), key.timelockNamespace()));
    }

    public static Authorizer of(Map<ClientId, Privileges> privileges, AuthRequirement authRequirement) {
        NamespaceLocker namespaceLocker = createNamespaceLocker(authRequirement, privileges);
        return new CachingAuthorizer(
                ImmutableMap.copyOf(privileges),
                namespaceLocker);
    }

    @Override
    public boolean isAuthorized(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        return cache.get(ImmutableCacheKey.of(authenticatedClient, namespace));
    }

    private boolean isAuthorizedInternal(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        return !namespaceLocker.isLocked(namespace)
                || privileges.getOrDefault(authenticatedClient.id(), Privileges.EMPTY).hasPrivilege(namespace);
    }

    private static NamespaceLocker createNamespaceLocker(
            AuthRequirement authRequirement,
            Map<ClientId, Privileges> privileges) {
        switch (authRequirement) {
            case NEVER_REQUIRE:
                return NamespaceLocker.NONE_LOCKED;
            case ALWAYS_REQUIRE:
                return NamespaceLocker.ALL_LOCKED;
            case PRIVILEGE_BASED:
                return NamespaceLocker.deriveFromPrivileges(privileges);
            default:
                throw new IllegalStateException("unknown auth requirement option " + authRequirement);
        }
    }


    @Value.Immutable
    interface CacheKey {
        @Value.Parameter
        AuthenticatedClient authenticatedClient();

        @Value.Parameter
        TimelockNamespace timelockNamespace();
    }
}
