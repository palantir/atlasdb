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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;

public class PrivilegeBasedNamespaceLocker implements NamespaceLocker {
    private List<Privileges> nonAdminPrivileges;

    PrivilegeBasedNamespaceLocker(Map<ClientId, Privileges> privileges) {
        this.nonAdminPrivileges = privileges.values().stream()
                .filter(privilege -> privilege != Privileges.ADMIN)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isLocked(TimelockNamespace namespace) {
        return nonAdminPrivileges.stream().anyMatch(privilege -> privilege.hasPrivilege(namespace));
    }
}
