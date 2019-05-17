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
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.TimelockNamespace;
import com.palantir.atlasdb.timelock.auth.api.Authorizer;
import com.palantir.atlasdb.timelock.auth.api.NamespaceMatcher;
import com.palantir.atlasdb.timelock.auth.api.Client;

public class SimpleAuthorizer implements Authorizer {
    private final Map<Client, NamespaceMatcher> privileges;
    private final Set<Client> admins;
    private final AuthRequirer authRequirer;

    @VisibleForTesting
    SimpleAuthorizer(Map<Client, NamespaceMatcher> privileges, Set<Client> admins, AuthRequirer authRequirer) {
        this.privileges = privileges;
        this.admins = admins;
        this.authRequirer = authRequirer;
    }

    public static Authorizer of(Map<Client, NamespaceMatcher> privileges, Set<Client> admins,
            AuthRequirement authRequirement) {
        AuthRequirer authRequirer = getAuthRequirer(authRequirement, privileges);
        return new SimpleAuthorizer(
                ImmutableMap.copyOf(privileges),
                ImmutableSet.copyOf(admins),
                authRequirer);
    }

    @Override
    public boolean isAuthorized(Client client, TimelockNamespace namespace) {
        return !isAuthorizationRequired(namespace) || isAdmin(client)
                || privileges.getOrDefault(client, NamespaceMatcher.NEVER_MATCH).matches(namespace);
    }

    private static AuthRequirer getAuthRequirer(AuthRequirement authRequirement, Map<Client, NamespaceMatcher> privileges) {
        switch (authRequirement) {
            case NEVER:
                return AuthRequirer.NEVER_REQUIRE;
            case ALWAYS:
                return AuthRequirer.ALWAYS_REQUIRE;
            case PRIVILEGE_BASED:
                return AuthRequirer.deriveFromPrivileges(privileges);
            default:
                throw new IllegalStateException("unknown auth requirement option " + authRequirement);
        }
    }

    private boolean isAuthorizationRequired(TimelockNamespace namespace) {
        return authRequirer.requiresAuth(namespace);
    }

    private boolean isAdmin(Client client) {
        return admins.contains(client);
    }
}
