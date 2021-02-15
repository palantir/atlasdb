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

import com.palantir.atlasdb.timelock.auth.api.AuthenticatedClient;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SimpleAuthorizerTest {
    private static final TimelockNamespace NAMESPACE_1 = TimelockNamespace.of("namespace_1");
    private static final TimelockNamespace NAMESPACE_2 = TimelockNamespace.of("namespace_2");

    private static final ClientId CLIENT_1 = ClientId.of("user_1");
    private static final ClientId CLIENT_2 = ClientId.of("user_2");
    private static final ClientId ADMIN_ID = ClientId.of("admin");

    private static final AuthenticatedClient AUTHENTICATED_CLIENT_1 = AuthenticatedClient.create(CLIENT_1);
    private static final AuthenticatedClient AUTHENTICATED_CLIENT_2 = AuthenticatedClient.create(CLIENT_2);
    private static final AuthenticatedClient ADMIN = AuthenticatedClient.create(ADMIN_ID);

    @Mock
    private NamespaceLocker namespaceLocker;

    private Map<ClientId, Privileges> privileges = new HashMap<>();

    @Test
    public void adminIsAlwaysAuthorized() {
        withLockedNamespace(NAMESPACE_1);
        withAdmin(ADMIN_ID);

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
        withPrivilege(CLIENT_1, NAMESPACE_1);

        assertAuthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_1);
        assertUnauthorized(AUTHENTICATED_CLIENT_2, NAMESPACE_1);
        assertUnauthorized(AuthenticatedClient.ANONYMOUS, NAMESPACE_1);
    }

    @Test
    public void usersOnlyHaveAccessToTheirNamespaces() {
        withLockedNamespace(NAMESPACE_1);
        withLockedNamespace(NAMESPACE_2);
        withPrivilege(CLIENT_1, NAMESPACE_1);
        withPrivilege(CLIENT_2, NAMESPACE_2);

        assertAuthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_1);
        assertAuthorized(AUTHENTICATED_CLIENT_2, NAMESPACE_2);

        assertUnauthorized(AUTHENTICATED_CLIENT_1, NAMESPACE_2);
        assertUnauthorized(AUTHENTICATED_CLIENT_2, NAMESPACE_1);
    }

    private void assertAuthorized(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        assertThat(new SimpleAuthorizer(privileges, namespaceLocker).isAuthorized(authenticatedClient, namespace))
                .isTrue();
    }

    private void assertUnauthorized(AuthenticatedClient authenticatedClient, TimelockNamespace namespace) {
        assertThat(new SimpleAuthorizer(privileges, namespaceLocker).isAuthorized(authenticatedClient, namespace))
                .isFalse();
    }

    private void withUnlockedNamespace(TimelockNamespace namespace) {
        when(namespaceLocker.isLocked(namespace)).thenReturn(false);
    }

    private void withLockedNamespace(TimelockNamespace namespace) {
        when(namespaceLocker.isLocked(namespace)).thenReturn(true);
    }

    private void withAdmin(ClientId clientId) {
        privileges.put(clientId, Privileges.ADMIN);
    }

    private void withPrivilege(ClientId clientId, TimelockNamespace namespace) {
        Privileges existingMatcher = privileges.getOrDefault(clientId, Privileges.EMPTY);
        privileges.put(clientId, n -> existingMatcher.hasPrivilege(n) || n.equals(namespace));
    }
}
