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

package com.palantir.atlasdb.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuppressedExceptionTest {
    private static final Logger log = LoggerFactory.getLogger("AnnotatedCallable");

    @Test
    public void throwableSuppressingItselfShouldBeLoggable() {
        // When logging exceptions with circular dependencies, logback can cause stack overflow. See
        // https://jira.qos.ch/browse/LOGBACK-1027. This tests asserts that there are no circular dependencies when
        // using SuppressedException, i.e., logback logs successfully.

        Throwable throwable = new Throwable("a exception");
        throwable.addSuppressed(SuppressedException.from(throwable));
        log.error("Error", throwable);
    }
}
