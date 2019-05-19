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

import com.palantir.atlasdb.timelock.auth.api.AuthenticatedClient;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;

@RunWith(MockitoJUnitRunner.class)
public class SimpleAuthorizerTest {
    private static final TimelockNamespace NAMESPACE_1 = TimelockNamespace.of("namespace_1");
    private static final TimelockNamespace NAMESPACE_2 = TimelockNamespace.of("namespace_2");

    private static final AuthenticatedClient AUTHENTICATED_CLIENT_1 = AuthenticatedClient.create("user_1");
    private static final AuthenticatedClient AUTHENTICATED_CLIENT_2 = AuthenticatedClient.create("user_2");
    private static final AuthenticatedClient ADMIN = AuthenticatedClient.create("admin");

    @Mock
    private NamespaceLocker namespaceLocker;
    private Map<AuthenticatedClient, Privileges> privileges = new HashMap<>();

    @Test
    public void adminIsAlwaysAuthorized() {
        withLockedNamespace(NAMESPACE_1);
        withAdmin(ADMIN);

        assertAuthorized(ADMIN, NAMESPACE_1);
    }

    @Test
    public void authorizesUsersIfNamespaceDoesNotRequireAuth() {
        withUnlockedNamespace(NAMESPACE_1);

        assertAuthorized(AuthenticatedClient.ANONYMOUS, NAMESPACE_1);
        assertAuthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_1);
    }

    @Test
    public void onlyAllowAuthorizedUsersIfNamespaceRequiresAuth() {
        withLockedNamespace(NAMESPACE_1);
        withPrivilege(AUTHENTICATED_CLIENT_1, NAMESPACE_1);

        assertAuthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_1);
        assertUnauthorized(AUTHENTICATED_CLIENT_2, NAMESPACE_1);
        assertUnauthorized(AuthenticatedClient.ANONYMOUS, NAMESPACE_1);
    }

    @Test
    public void usersOnlyHaveAccessToTheirNamespaces() {
        withLockedNamespace(NAMESPACE_1);
        withLockedNamespace(NAMESPACE_2);
        withPrivilege(AUTHENTICATED_CLIENT_1, NAMESPACE_1);
        withPrivilege(AUTHENTICATED_CLIENT_2, NAMESPACE_2);

        assertAuthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_1);
        assertAuthorized(AUTHENTICATED_CLIENT_2, NAMESPACE_2);

        assertUnauthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_2);
        assertUnauthorized(AUTHENTICATED_CLIENT_2, NAMESPACE_1);
    }

    private void assertAuthorized(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        assertThat(new SimpleAuthorizer(privileges, namespaceLocker).isAuthorized(authenticatedClient, namespace)).isTrue();
    }

    private void assertUnauthorized(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        assertThat(new SimpleAuthorizer(privileges, namespaceLocker).isAuthorized(authenticatedClient, namespace)).isFalse();
    }

    private void withUnlockedNamespace(TimelockNamespace namespace) {
        when(namespaceLocker.isLocked(namespace)).thenReturn(false);
    }

    private void withLockedNamespace(TimelockNamespace namespace) {
        when(namespaceLocker.isLocked(namespace)).thenReturn(true);
    }

    private void withAdmin(AuthenticatedClient authenticatedClient) {
        privileges.put(authenticatedClient, Privileges.ADMIN);
    }

    private void withPrivilege(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        Privileges existingMatcher = privileges.getOrDefault(authenticatedClient, Privileges.EMPTY);
        privileges.put(authenticatedClient, n -> existingMatcher.hasPrivilege(n) || n.equals(namespace));
    }
}
