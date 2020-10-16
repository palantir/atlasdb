/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class TimeLockAgentTest {
    private final PersistedSchemaVersion schemaVersion = mock(PersistedSchemaVersion.class);

    @Test
    public void throwWhenPersistedSchemaVersionTooLow() {
        when(schemaVersion.getVersion()).thenReturn(TimeLockAgent.SCHEMA_VERSION - 1);
        assertThatThrownBy(() -> TimeLockAgent.verifySchemaVersion(schemaVersion))
                .as("Persisted version lower than current")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void throwWhenPersistedSchemaVersionTooHigh() {
        when(schemaVersion.getVersion()).thenReturn(TimeLockAgent.SCHEMA_VERSION + 1);
        assertThatThrownBy(() -> TimeLockAgent.verifySchemaVersion(schemaVersion))
                .as("Persisted version higher than current")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void doNotThrowWhenPersistedSchemaVersionEqual() {
        when(schemaVersion.getVersion()).thenReturn(TimeLockAgent.SCHEMA_VERSION);
        assertThatCode(() -> TimeLockAgent.verifySchemaVersion(schemaVersion))
                .as("Persisted version lower than current")
                .doesNotThrowAnyException();
    }
}
