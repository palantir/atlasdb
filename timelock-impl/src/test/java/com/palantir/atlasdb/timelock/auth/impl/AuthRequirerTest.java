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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.timelock.auth.api.NamespaceMatcher;
import com.palantir.atlasdb.timelock.auth.api.User;

public class AuthRequirerTest {
    private static final User USER = User.of("user");

    private static final Namespace NAMESPACE_1 = Namespace.create("namespace_1");
    private static final Namespace NAMESPACE_2 = Namespace.create("namespace_2");

    @Test
    public void shouldRequireAuthIfPrivilegeIsSpecified() {
        Map<User, NamespaceMatcher> privilege = ImmutableMap.of(USER, createMatcherFor(NAMESPACE_1));
        AuthRequirer authRequirer = AuthRequirer.deriveFromPrivileges(privilege);

        assertThat(authRequirer.requiresAuth(NAMESPACE_1)).isTrue();
        assertThat(authRequirer.requiresAuth(NAMESPACE_2)).isFalse();
    }

    @Test
    public void ALWAYS_REQUIRE_shouldAlwaysRequireAuth() {
        AuthRequirer authRequirer = AuthRequirer.ALWAYS_REQUIRE;

        assertThat(authRequirer.requiresAuth(NAMESPACE_1)).isTrue();
        assertThat(authRequirer.requiresAuth(NAMESPACE_2)).isTrue();
    }

    @Test
    public void NEVER_REQUIRE_shouldNeverRequireAuth() {
        AuthRequirer authRequirer = AuthRequirer.NEVER_REQUIRE;

        assertThat(authRequirer.requiresAuth(NAMESPACE_1)).isFalse();
        assertThat(authRequirer.requiresAuth(NAMESPACE_2)).isFalse();
    }

    private NamespaceMatcher createMatcherFor(Namespace namespace) {
        return otherNamespace -> otherNamespace.equals(namespace);
    }
}