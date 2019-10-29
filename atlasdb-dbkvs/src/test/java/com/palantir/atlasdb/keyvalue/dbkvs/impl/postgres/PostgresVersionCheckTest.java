/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class PostgresVersionCheckTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    @SuppressWarnings(value = "Slf4jConstantLogMessage")
    public void shouldLogErrorOn_9_2_24() {
        verifyLowVersionLogsError("9.2.24");
    }

    @Test
    public void shouldLogErrorOn_9_5_2() {
        verifyLowVersionLogsError("9.5.2");
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    private void verifyLowVersionLogsError(String lowVersion) {
        Logger log = mock(Logger.class);
        String expectedMessage = "The minimum supported version is " + PostgresVersionCheck.MIN_POSTGRES_VERSION;
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion(lowVersion, log))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(expectedMessage);
        verify(log).error(
                eq("Assertion {} with exception "),
                contains(expectedMessage),
                Mockito.any(Exception.class));
        verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldFailOn_9_5_0() {
        thrown.expectMessage("Versions 9.5.0 and 9.5.1 contain a known bug");
        PostgresVersionCheck.checkDatabaseVersion("9.5.0", mock(Logger.class));
    }

    @Test
    public void shouldFailOn_9_5_1() {
        thrown.expectMessage("Versions 9.5.0 and 9.5.1 contain a known bug");
        PostgresVersionCheck.checkDatabaseVersion("9.5.1", mock(Logger.class));
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void shouldBeFineOn_9_6_12() {
        Logger log = mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.6.12", log);
        verifyNoMoreInteractions(log);
    }

}
