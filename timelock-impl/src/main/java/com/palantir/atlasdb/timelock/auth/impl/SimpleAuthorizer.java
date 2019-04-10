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
import com.palantir.atlasdb.timelock.auth.api.User;

public class SimpleAuthorizer implements Authorizer {
    private final Map<User, NamespaceMatcher> privileges;
    private final AuthRequirer authRequirer;

    SimpleAuthorizer(Map<User, NamespaceMatcher> privileges, AuthRequirer authRequirer) {
        this.privileges = privileges;
        this.authRequirer = authRequirer;
    }

    public static Authorizer of(Map<User, NamespaceMatcher> privileges, AuthRequirer authRequirer) {
        return new SimpleAuthorizer(new HashMap<>(privileges), authRequirer);
    }

    @Override
    public boolean isAuthorized(User user, Namespace namespace) {
        if (!isAuthorizationRequired(namespace) || user.isAdmin()) {
            return true;
        }

        return privileges.getOrDefault(user, NamespaceMatcher.ALWAYS_DENY).matches(namespace);
    }

    private boolean isAuthorizationRequired(Namespace namespace) {
        return authRequirer.requiresAuth(namespace);
    }
}
