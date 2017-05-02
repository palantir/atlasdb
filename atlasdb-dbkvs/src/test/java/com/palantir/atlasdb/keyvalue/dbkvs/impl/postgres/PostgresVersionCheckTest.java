/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.slf4j.Logger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class PostgresVersionCheckTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    @SuppressFBWarnings(value = "SLF4J_FORMAT_SHOULD_BE_CONST", justification = "I want to use a mockito matcher")
    public void shouldLogErrorOn_9_1_24() {
        Logger log = Mockito.mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.1.24", log);
        Mockito.verify(log).error(Mockito.argThat(new ArgumentMatcher<String>() {
            @Override
            public boolean matches(Object arg) {
                return ((String) arg).contains("The minimum supported version is {}");
            }
        }), Mockito.anyObject(), Mockito.eq("9.2"));
        Mockito.verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldBeFineOn_9_2() {
        Logger log = Mockito.mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.2", log);
        Mockito.verifyNoMoreInteractions(log);
    }

    @Test
    public void shouldFailOn_9_5() {
        thrown.expectMessage("Versions 9.5.0 and 9.5.1 contain a known bug");
        PostgresVersionCheck.checkDatabaseVersion("9.5", Mockito.mock(Logger.class));
    }

    @Test
    public void shouldFailOn_9_5_0() {
        thrown.expectMessage("Versions 9.5.0 and 9.5.1 contain a known bug");
        PostgresVersionCheck.checkDatabaseVersion("9.5.0", Mockito.mock(Logger.class));
    }

    @Test
    public void shouldFailOn_9_5_1() {
        thrown.expectMessage("Versions 9.5.0 and 9.5.1 contain a known bug");
        PostgresVersionCheck.checkDatabaseVersion("9.5.1", Mockito.mock(Logger.class));
    }

    @Test
    public void shouldBeFineOn_9_5_2() {
        Logger log = Mockito.mock(Logger.class);
        PostgresVersionCheck.checkDatabaseVersion("9.5.2", log);
        Mockito.verifyNoMoreInteractions(log);
    }

}
