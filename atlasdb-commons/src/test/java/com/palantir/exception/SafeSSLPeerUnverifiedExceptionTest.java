/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.logsafe.UnsafeArg;
import org.junit.Test;

public class SafeSSLPeerUnverifiedExceptionTest {

    private static final String badArgKey = "bad-arg";
    private static final String badArgValue = "iamdangerous!";
    private static final UnsafeArg<String> unsafeArg = UnsafeArg.of(badArgKey, badArgValue);

    @Test
    public void logMessage_doesNotHaveUnsafeArgs() {
        assertThat(new SafeSSLPeerUnverifiedException("foo", unsafeArg).getLogMessage())
                .doesNotContain(badArgValue);
    }

    @Test
    public void getArgs_hasProvidedArgs() {
        assertThat(new SafeSSLPeerUnverifiedException("foo", unsafeArg).getArgs())
                .containsExactly(unsafeArg);
    }
}
