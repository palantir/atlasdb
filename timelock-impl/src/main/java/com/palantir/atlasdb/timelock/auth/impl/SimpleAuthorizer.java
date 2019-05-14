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

import java.util.HashMap;
import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.timelock.auth.api.Authorizer;
import com.palantir.atlasdb.timelock.auth.api.NamespaceMatcher;
import com.palantir.atlasdb.timelock.auth.api.Client;

public class SimpleAuthorizer implements Authorizer {
    private final Map<Client, NamespaceMatcher> privileges;
    private final AuthRequirer authRequirer;

    SimpleAuthorizer(Map<Client, NamespaceMatcher> privileges, AuthRequirer authRequirer) {
        this.privileges = privileges;
        this.authRequirer = authRequirer;
    }

    public static Authorizer of(Map<Client, NamespaceMatcher> privileges, AuthRequirer authRequirer) {
        return new SimpleAuthorizer(new HashMap<>(privileges), authRequirer);
    }

    @Override
    public boolean isAuthorized(Client client, Namespace namespace) {
        if (!isAuthorizationRequired(namespace) || client.isAdmin()) {
            return true;
        }

        return privileges.getOrDefault(client, NamespaceMatcher.ALWAYS_DENY).matches(namespace);
    }

    private boolean isAuthorizationRequired(Namespace namespace) {
        return authRequirer.requiresAuth(namespace);
    }
}
