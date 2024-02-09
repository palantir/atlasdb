/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.junit.jupiter.api.Test;

class ClientTest {

    @Test
    void successCases() {
        assertThatCode(() -> Client.of("lowercase")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("UPPERCASE")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("123449")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("____")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("----")).doesNotThrowAnyException();

        assertThatCode(() -> Client.of("l.")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("1.")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("-.")).doesNotThrowAnyException();
        assertThatCode(() -> Client.of("_.")).doesNotThrowAnyException();
    }

    @Test
    void failureCases() {
        assertThatLoggableExceptionThrownBy(() -> Client.of(".hello"))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .message()
                .startsWith("Error parsing client as it doesn't match pattern");

        assertThatLoggableExceptionThrownBy(() -> Client.of("?hello"))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .message()
                .startsWith("Error parsing client as it doesn't match pattern");

        assertThatLoggableExceptionThrownBy(() -> Client.of("!hello"))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .message()
                .startsWith("Error parsing client as it doesn't match pattern");
    }
}