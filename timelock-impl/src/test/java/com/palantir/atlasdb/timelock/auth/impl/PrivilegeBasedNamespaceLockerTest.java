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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;
import java.util.Map;
import org.junit.Test;

public class PrivilegeBasedNamespaceLockerTest {
    private static final ClientId AUTHENTICATED_CLIENT = ClientId.of("user");

    private static final TimelockNamespace NAMESPACE_1 = TimelockNamespace.of("namespace_1");
    private static final TimelockNamespace NAMESPACE_2 = TimelockNamespace.of("namespace_2");

    @Test
    public void shouldRequireAuthIfPrivilegeIsSpecified() {
        Map<ClientId, Privileges> privilege = ImmutableMap.of(AUTHENTICATED_CLIENT, createPrivilegesFor(NAMESPACE_1));
        NamespaceLocker namespaceLocker = NamespaceLocker.deriveFromPrivileges(privilege);

        assertThat(namespaceLocker.isLocked(NAMESPACE_1)).isTrue();
        assertThat(namespaceLocker.isLocked(NAMESPACE_2)).isFalse();
    }

    private Privileges createPrivilegesFor(TimelockNamespace namespace) {
        return otherNamespace -> otherNamespace.equals(namespace);
    }
}