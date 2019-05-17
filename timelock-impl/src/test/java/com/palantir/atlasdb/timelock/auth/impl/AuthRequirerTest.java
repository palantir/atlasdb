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
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;
import com.palantir.atlasdb.timelock.auth.api.Client;
import com.palantir.atlasdb.timelock.auth.api.NamespaceMatcher;

public class AuthRequirerTest {
    private static final Client CLIENT = Client.create("user");

    private static final TimelockNamespace NAMESPACE_1 = TimelockNamespace.of("namespace_1");
    private static final TimelockNamespace NAMESPACE_2 = TimelockNamespace.of("namespace_2");

    @Test
    public void shouldRequireAuthIfPrivilegeIsSpecified() {
        Map<Client, Privileges> privilege = ImmutableMap.of(CLIENT, createMatcherFor(NAMESPACE_1));
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

    private Privileges createMatcherFor(TimelockNamespace namespace) {
        return otherNamespace -> otherNamespace.equals(namespace);
    }
}