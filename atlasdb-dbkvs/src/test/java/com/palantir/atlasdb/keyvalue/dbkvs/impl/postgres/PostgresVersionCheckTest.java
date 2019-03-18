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

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;

import org.junit.Assert;
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
        Logger log = Mockito.mock(Logger.class);
        String expectedMessage = "The minimum supported version is " + PostgresVersionCheck.MIN_POSTGRES_VERSION;
        try {
            PostgresVersionCheck.checkDatabaseVersion("9.2.24", log);
            Assert.fail("Expected an AssertionError");
        } catch (AssertionError error) {
            Assert.assertTrue("Error did not contain expected message. Actual error: " + error,
                    error.getMessage().contains(expectedMessage));
        }
        Mockito.verify(log).error(
                eq("Assertion {} with exception "),
                contains(expectedMessage),
                Mockito.any(Exception.class));
        Mockito.verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldBeFineOn_9_5_2() {
        Logger log = Mockito.mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.5.2", log);
        Mockito.verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldBeFineOn_9_6_12() {
        Logger log = Mockito.mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.6.12", log);
        Mockito.verifyNoMoreInteractions(log);
    }

}
