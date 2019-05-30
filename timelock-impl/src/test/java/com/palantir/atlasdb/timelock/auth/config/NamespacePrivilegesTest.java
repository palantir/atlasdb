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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;

public class NamespacePrivilegesTest {
    private static final TimelockNamespace NAMESPACE_1 = TimelockNamespace.of("namespace-1");
    private static final TimelockNamespace NAMESPACE_2 = TimelockNamespace.of("namespace-2");

    private static final String SUFFIX = "bar";

    @Test
    public void privilegesAreGrantedCorrectly() {
        Privileges namespacePrivileges = NamespacePrivileges.of(
                ImmutableSet.of(NAMESPACE_1),
                ImmutableSet.of(SUFFIX));

        assertThat(namespacePrivileges.hasPrivilege(NAMESPACE_1)).isTrue();
        assertThat(namespacePrivileges.hasPrivilege(TimelockNamespace.of("foo" + SUFFIX))).isTrue();

        assertThat(namespacePrivileges.hasPrivilege(NAMESPACE_2)).isFalse();
    }
}
