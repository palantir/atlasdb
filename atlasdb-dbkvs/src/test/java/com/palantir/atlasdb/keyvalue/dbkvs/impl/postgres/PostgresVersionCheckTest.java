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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import org.junit.Test;
import org.mockito.Mockito;

public class PostgresVersionCheckTest {

    @Test
    @SuppressWarnings(value = "Slf4jConstantLogMessage")
    public void shouldLogErrorOn_9_2_24() {
        verifyLowVersionLogsError("9.2.24");
    }

    @Test
    public void shouldLogErrorOn_9_5_2() {
        verifyLowVersionLogsError("9.5.2");
    }

    @SuppressWarnings("CompileTimeConstant")
    private static void verifyLowVersionLogsError(String lowVersion) {
        SafeLogger log = mock(SafeLogger.class);
        String expectedMessage = "The minimum supported version is";
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion(lowVersion, log))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(expectedMessage);
        verify(log)
                .error(
                        eq("An error occurred"),
                        eq(ImmutableList.of(
                                SafeArg.of(
                                        "message",
                                        "Your key value service currently uses version %s of postgres."
                                                + " The minimum supported version is %s."
                                                + " If you absolutely need to use an older version of postgres,"
                                                + " please contact Palantir support for assistance."),
                                SafeArg.of("version", lowVersion),
                                SafeArg.of("minVersion", PostgresVersionCheck.MIN_POSTGRES_VERSION))),
                        Mockito.any(Exception.class));
        verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldFailOn_9_5_0() {
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion("9.5.0", mock(SafeLogger.class)))
                .hasMessageContaining("Versions 9.5.0 and 9.5.1 contain a known bug");
    }

    @Test
    public void shouldFailOn_9_5_1() {
        assertThatThrownBy(() -> PostgresVersionCheck.checkDatabaseVersion("9.5.1", mock(SafeLogger.class)))
                .hasMessageContaining("Versions 9.5.0 and 9.5.1 contain a known bug");
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void shouldBeFineOn_9_6_12() {
        SafeLogger log = mock(SafeLogger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.6.12", log);
        verifyNoMoreInteractions(log);
    }
}
