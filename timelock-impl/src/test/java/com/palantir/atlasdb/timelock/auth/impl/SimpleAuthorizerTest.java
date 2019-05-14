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
import com.palantir.atlasdb.timelock.auth.api.Client;
import com.palantir.atlasdb.timelock.auth.api.NamespaceMatcher;

@RunWith(MockitoJUnitRunner.class)
public class SimpleAuthorizerTest {
    private static final Namespace NAMESPACE_1 = Namespace.create("namespace_1");
    private static final Namespace NAMESPACE_2 = Namespace.create("namespace_2");

    private static final Client CLIENT_1 = Client.of("user_1");
    private static final Client CLIENT_2 = Client.of("user_2");
    private static final Client ADMIN = Client.createAdmin("admin");

    @Mock
    private AuthRequirer authRequirer;
    private Map<Client, NamespaceMatcher> privileges = new HashMap<>();

    @Test
    public void adminIsAlwaysAuthorized() {
        withLockedNamespace(NAMESPACE_1);
        assertAuthorized(ADMIN, NAMESPACE_1);
    }

    @Test
    public void authorizesUsersIfNamespaceDoesNotRequireAuth() {
        withUnlockedNamespace(NAMESPACE_1);

        assertAuthorized(Client.ANONYMOUS, NAMESPACE_1);
        assertAuthorized(CLIENT_1, NAMESPACE_1);
    }

    @Test
    public void onlyAllowAuthorizedUsersIfNamespaceRequiresAuth() {
        withLockedNamespace(NAMESPACE_1);
        withPrivilege(CLIENT_1, NAMESPACE_1);

        assertAuthorized(CLIENT_1, NAMESPACE_1);
        assertUnauthorized(CLIENT_2, NAMESPACE_1);
        assertUnauthorized(Client.ANONYMOUS, NAMESPACE_1);
    }

    @Test
    public void usersOnlyHaveAccessToTheirNamespaces() {
        withLockedNamespace(NAMESPACE_1);
        withLockedNamespace(NAMESPACE_2);
        withPrivilege(CLIENT_1, NAMESPACE_1);
        withPrivilege(CLIENT_2, NAMESPACE_2);

        assertAuthorized(CLIENT_1, NAMESPACE_1);
        assertAuthorized(CLIENT_2, NAMESPACE_2);

        assertUnauthorized(CLIENT_1, NAMESPACE_2);
        assertUnauthorized(CLIENT_2, NAMESPACE_1);
    }

    private void assertAuthorized(Client client, Namespace namespace) {
        assertThat(SimpleAuthorizer.of(privileges, authRequirer).isAuthorized(client, namespace)).isTrue();
    }

    private void assertUnauthorized(Client client, Namespace namespace) {
        assertThat(SimpleAuthorizer.of(privileges, authRequirer).isAuthorized(client, namespace)).isFalse();
    }

    private void withUnlockedNamespace(Namespace namespace) {
        when(authRequirer.requiresAuth(namespace)).thenReturn(false);
    }

    private void withLockedNamespace(Namespace namespace) {
        when(authRequirer.requiresAuth(namespace)).thenReturn(true);
    }

    private void withPrivilege(Client client, Namespace namespace) {
        NamespaceMatcher existingMatcher = privileges.getOrDefault(client, NamespaceMatcher.NEVER_MATCH);
        privileges.put(client, n -> existingMatcher.matches(n) || n.equals(namespace));
    }
}
