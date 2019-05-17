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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;
import com.palantir.atlasdb.timelock.auth.api.Authorizer;
import com.palantir.atlasdb.timelock.auth.api.Client;

public class SimpleAuthorizer implements Authorizer {
    private final Map<Client, Privileges> privileges;
    private final NamespaceLocker namespaceLocker;

    @VisibleForTesting
    SimpleAuthorizer(Map<Client, Privileges> privileges, NamespaceLocker namespaceLocker) {
        this.privileges = privileges;
        this.namespaceLocker = namespaceLocker;
    }

    public static Authorizer of(Map<Client, Privileges> privileges, AuthRequirement authRequirement) {
        NamespaceLocker namespaceLocker = getAuthRequirer(authRequirement, privileges);
        return new SimpleAuthorizer(
                ImmutableMap.copyOf(privileges),
                namespaceLocker);
    }

    @Override
    public boolean isAuthorized(Client client, TimelockNamespace namespace) {
        return !namespaceLocker.isLocked(namespace)
                || privileges.getOrDefault(client, Privileges.EMPTY).hasPrivilege(namespace);
    }

    private static NamespaceLocker getAuthRequirer(AuthRequirement authRequirement, Map<Client, Privileges> privileges) {
        switch (authRequirement) {
            case NEVER:
                return NamespaceLocker.NONE_LOCKED;
            case ALWAYS:
                return NamespaceLocker.ALL_LOCKED;
            case PRIVILEGE_BASED:
                return NamespaceLocker.deriveFromPrivileges(privileges);
            default:
                throw new IllegalStateException("unknown auth requirement option " + authRequirement);
        }
    }
}
