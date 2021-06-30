/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.junit.Test;

public class PermittedKeyValueServiceTypesTest {
    @Test
    public void allExpectedTypesArePermitted() {
        assertThatCode(() -> PermittedKeyValueServiceTypes.checkKeyValueServiceTypeIsPermitted("relational"))
                .doesNotThrowAnyException();
        assertThatCode(() -> PermittedKeyValueServiceTypes.checkKeyValueServiceTypeIsPermitted("memory"))
                .doesNotThrowAnyException();
    }

    @Test
    public void cassandraIsNotPermitted() {
        assertKeyValueServiceTypeNotPermitted("cassandra");
    }

    @Test
    public void unknownTypesNotPermitted() {
        assertKeyValueServiceTypeNotPermitted("");
        assertKeyValueServiceTypeNotPermitted("s3");
    }

    public static void assertKeyValueServiceTypeNotPermitted(String typeIdentifier) {
        assertThatThrownBy(() -> PermittedKeyValueServiceTypes.checkKeyValueServiceTypeIsPermitted(typeIdentifier))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Only InMemory/Dbkvs is a supported for TimeLock's database persister.");
    }
}
