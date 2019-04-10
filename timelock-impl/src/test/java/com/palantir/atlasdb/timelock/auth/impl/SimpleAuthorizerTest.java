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
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.timelock.auth.api.NamespaceMatcher;
import com.palantir.atlasdb.timelock.auth.api.User;

@RunWith(MockitoJUnitRunner.class)
public class SimpleAuthorizerTest {
    private static final Namespace NAMESPACE_1 = Namespace.create("namespace_1");
    private static final Namespace NAMESPACE_2 = Namespace.create("namespace_2");

    private static final User USER_1 = User.of("user_1");
    private static final User USER_2 = User.of("user_2");
    private static final User ADMIN = User.createAdmin("admin");

    @Mock
    private AuthRequirer authRequirer;
    private Map<User, NamespaceMatcher> privileges = new HashMap<>();

    @Test
    public void adminIsAlwaysAuthorized() {
        withLockedNamespace(NAMESPACE_1);
        assertAuthorized(ADMIN, NAMESPACE_1);
    }

    @Test
    public void authorizesUsersIfNamespaceDoesNotRequireAuth() {
        withUnlockedNamespace(NAMESPACE_1);

        assertAuthorized(User.ANONYMOUS, NAMESPACE_1);
        assertAuthorized(USER_1, NAMESPACE_1);
    }

    @Test
    public void onlyAllowAuthorizedUsersIfNamespaceRequiresAuth() {
        withLockedNamespace(NAMESPACE_1);
        withPrivilege(USER_1, NAMESPACE_1);

        assertAuthorized(USER_1, NAMESPACE_1);
        assertUnauthorized(USER_2, NAMESPACE_1);
        assertUnauthorized(User.ANONYMOUS, NAMESPACE_1);
    }

    @Test
    public void usersOnlyHaveAccessToTheirNamespaces() {
        withLockedNamespace(NAMESPACE_1);
        withLockedNamespace(NAMESPACE_2);
        withPrivilege(USER_1, NAMESPACE_1);
        withPrivilege(USER_2, NAMESPACE_2);

        assertAuthorized(USER_1, NAMESPACE_1);
        assertAuthorized(USER_2, NAMESPACE_2);

        assertUnauthorized(USER_1, NAMESPACE_2);
        assertUnauthorized(USER_2, NAMESPACE_1);
    }

    private void assertAuthorized(User user, Namespace namespace) {
        assertThat(SimpleAuthorizer.of(privileges, authRequirer).isAuthorized(user, namespace)).isTrue();
    }

    private void assertUnauthorized(User user, Namespace namespace) {
        assertThat(SimpleAuthorizer.of(privileges, authRequirer).isAuthorized(user, namespace)).isFalse();
    }

    private void withUnlockedNamespace(Namespace namespace) {
        when(authRequirer.requiresAuth(namespace)).thenReturn(false);
    }

    private void withLockedNamespace(Namespace namespace) {
        when(authRequirer.requiresAuth(namespace)).thenReturn(true);
    }

    private void withPrivilege(User user, Namespace namespace) {
        NamespaceMatcher existingMatcher = privileges.getOrDefault(user, NamespaceMatcher.ALWAYS_DENY);
        privileges.put(user, n -> existingMatcher.matches(n) || n.equals(namespace));
    }
}
