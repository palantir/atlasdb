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

package com.palantir.atlasdb.timelock.auth.config;

import java.util.Set;
import java.util.stream.Collectors;

import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;

public class NamespacePrivileges implements Privileges {
    private Set<TimelockNamespace> namespaces;

    NamespacePrivileges(Set<TimelockNamespace> namespaces) {
        this.namespaces = namespaces;
    }

    static Privileges of(Set<String> namespaces) {
        Set<TimelockNamespace> timelockNamespaces = namespaces.stream()
                .map(TimelockNamespace::of)
                .collect(Collectors.toSet());
        return new NamespacePrivileges(timelockNamespaces);
    }

    @Override
    public boolean hasPrivilege(TimelockNamespace namespace) {
        return namespaces.contains(namespace);
    }
}
