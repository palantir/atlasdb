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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.palantir.logsafe.SafeArg;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class PostgresVersionCheckTest {

    @Test
    @SuppressWarnings(value = "Slf4jConstantLogMessage")
    public void shouldLogErrorOn_9_2_24() {
        verifyLowVersionLogsError("9.2.24", "9.2.24");
    }

    @Test
    public void shouldLogErrorOn_9_5_2() {
        verifyLowVersionLogsError("9.5.2", "9.5.2");
    }

    @Test
    public void shouldLogErrorOn_9_5_2_verbose() {
        verifyLowVersionLogsError("9.5.2 (Ubuntu 9.5.2.pgdg20.04+1)", "9.5.2");
    }

    @Test
    public void shouldLogErrorOnUnparseable() {
        verifyUnparseableVersionError("123A");
    }

    @Test
    public void shouldLogErrorOnEmpty() {
        verifyUnparseableVersionError("");
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    private static void verifyUnparseableVersionError(String version) {
        Logger log = mock(Logger.class);
        String expectedMessage = "Unable to parse a version from postgres";
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion(version, log))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(expectedMessage);
        verify(log)
                .error(
                        eq("Assertion with exception!"),
                        eq(version),
                        eq(PostgresVersionCheck.MIN_POSTGRES_VERSION),
                        isA(SafeArg.class),
                        Mockito.any(Exception.class));
        verifyNoMoreInteractions(log);
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    private static void verifyLowVersionLogsError(String lowVersion, String parsed) {
        Logger log = mock(Logger.class);
        String expectedMessage = "The minimum supported version is";
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion(lowVersion, log))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(expectedMessage);
        verify(log)
                .info(
                        eq("Parsed Postgres version"),
                        eq(SafeArg.of("parsed", parsed)),
                        eq(SafeArg.of("raw", lowVersion)));
        verify(log)
                .error(
                        eq("Assertion with exception!"),
                        eq(lowVersion),
                        eq(PostgresVersionCheck.extractValidPostgresVersion(lowVersion)),
                        eq(PostgresVersionCheck.MIN_POSTGRES_VERSION),
                        isA(SafeArg.class),
                        Mockito.any(Exception.class));
        verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldFailOn_9_5_0() {
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion("9.5.0", mock(Logger.class)))
                .hasMessageContaining("Versions 9.5.0 and 9.5.1 contain a known bug");
    }

    @Test
    public void shouldFailOn_9_5_1() {
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion("9.5.1", mock(Logger.class)))
                .hasMessageContaining("Versions 9.5.0 and 9.5.1 contain a known bug");
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void shouldBeFineOn_9_6_12() {
        Logger log = mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.6.12", log);
        verify(log)
                .info(
                        eq("Parsed Postgres version"),
                        eq(SafeArg.of("parsed", "9.6.12")),
                        eq(SafeArg.of("raw", "9.6.12")));
        verifyNoMoreInteractions(log);
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void shouldBeFineOn_14_11_verbose() {
        Logger log = mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("14.11 (Ubuntu 14.11-1.pgdg20.04+1)", log);
        verify(log)
                .info(
                        eq("Parsed Postgres version"),
                        eq(SafeArg.of("parsed", "14.11")),
                        eq(SafeArg.of("raw", "14.11 (Ubuntu 14.11-1.pgdg20.04+1)")));
        verifyNoMoreInteractions(log);
    }
}
