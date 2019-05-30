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

import org.immutables.value.Value;

import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;

@Value.Immutable
public abstract class NamespacePrivileges implements Privileges {
    abstract Set<TimelockNamespace> namespaces();
    abstract Set<String> namespaceSuffixes();

    static Privileges of(Set<TimelockNamespace> namespaces, Set<String> namespaceSuffixes) {
        return ImmutableNamespacePrivileges.builder()
                .addAllNamespaces(namespaces)
                .namespaceSuffixes(namespaceSuffixes)
                .build();
    }

    @Override
    public boolean hasPrivilege(TimelockNamespace namespace) {
        return namespaces().contains(namespace) || hasMatchingSuffix(namespace);
    }

    private boolean hasMatchingSuffix(TimelockNamespace namespace) {
        return namespaceSuffixes().stream().anyMatch(suffix -> namespace.value().endsWith(suffix));
    }
}
