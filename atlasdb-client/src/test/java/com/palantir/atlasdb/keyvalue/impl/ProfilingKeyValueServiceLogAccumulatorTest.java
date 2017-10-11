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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.After;
import org.junit.Test;

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;

public class ProfilingKeyValueServiceLogAccumulatorTest {
    private static final String LOG_TEMPLATE_1 = "The method {} was called.";
    private static final String LOG_TEMPLATE_2 = "Exception occurred: {}. {}.";

    private static final String LOG_TEMPLATE_WITH_ENDING_CURLY = "The user's stacktrace was {";
    private static final String LOG_TEMPLATE_WITH_STARTING_CURLY = "} character found at position {}";

    private static final Arg<String> ARG_1 = SafeArg.of("foo", "bar");
    private static final Arg<String> ARG_2 = SafeArg.of("baz", "quux");
    private static final Arg<String> ARG_3 = SafeArg.of("wat", "atlas");

    private final ProfilingKeyValueService.LoggingFunction logSink =
            mock(ProfilingKeyValueService.LoggingFunction.class);
    private final ProfilingKeyValueService.LogAccumulator accumulator =
            new ProfilingKeyValueService.LogAccumulator(logSink);

    @Test
    public void canAcceptLogs() {
        accumulator.log(LOG_TEMPLATE_1, ARG_1);
        accumulator.flush();
        verify(logSink).log(LOG_TEMPLATE_1, ARG_1);
    }

    @Test
    public void concatenatesLogsIfLoggingMultipleTimesBeforeFlush() {
        accumulator.log(LOG_TEMPLATE_1, ARG_1);
        accumulator.log(LOG_TEMPLATE_2, ARG_2, ARG_3);
        accumulator.flush();
        verify(logSink).log(eq(LOG_TEMPLATE_1 + LOG_TEMPLATE_2), eq(ARG_1), eq(ARG_2), eq(ARG_3));
    }

    @Test
    public void throwsOnLogTemplatesPossiblyCausingErrors() {
        assertThatThrownBy(() -> accumulator.log(LOG_TEMPLATE_WITH_ENDING_CURLY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.log(LOG_TEMPLATE_WITH_STARTING_CURLY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(logSink);
    }
}
