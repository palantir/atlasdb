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
package com.palantir.atlasdb.logging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import org.junit.After;
import org.junit.Test;

public class KvsProfilingLoggerTest {
    private static final String LOG_TEMPLATE_1 = "The method {} was called.";
    private static final String LOG_TEMPLATE_2 = "Exception occurred: {}. {}.";

    private static final Arg<String> ARG_1 = SafeArg.of("foo", "bar");
    private static final Arg<String> ARG_2 = SafeArg.of("baz", "quux");
    private static final Arg<String> ARG_3 = SafeArg.of("wat", "atlas");

    private final KvsProfilingLogger.LoggingFunction logSink =
            mock(KvsProfilingLogger.LoggingFunction.class);

    @Test
    public void propagatesLogsToSink() {
        try (KvsProfilingLogger.LogAccumulator accumulator =
                new KvsProfilingLogger.LogAccumulator(logSink)) {
            accumulator.log(LOG_TEMPLATE_1, ARG_1);
        }
        verify(logSink).log(LOG_TEMPLATE_1, ARG_1);
    }

    @Test
    public void concatenatesLogsWithNewlinesInBetweenIfLoggingMultipleTimesBeforeClose() {
        try (KvsProfilingLogger.LogAccumulator accumulator =
                new KvsProfilingLogger.LogAccumulator(logSink)) {
            accumulator.log(LOG_TEMPLATE_1, ARG_1);
            accumulator.log(LOG_TEMPLATE_2, ARG_2, ARG_3);
        }
        verify(logSink).log(eq(LOG_TEMPLATE_1 + "\n" + LOG_TEMPLATE_2), eq(ARG_1), eq(ARG_2), eq(ARG_3));
    }

    @Test
    public void doesNotLogAnythingIfNotClosed() {
        KvsProfilingLogger.LogAccumulator accumulator = new KvsProfilingLogger.LogAccumulator(logSink);
        accumulator.log(LOG_TEMPLATE_1, ARG_1);
        verify(logSink, never()).log(any(), any());
    }

    @Test
    public void logsOnlyOnceEvenIfClosedMultipleTimes() {
        KvsProfilingLogger.LogAccumulator accumulator = new KvsProfilingLogger.LogAccumulator(logSink);
        accumulator.log(LOG_TEMPLATE_1, ARG_1);
        accumulator.close();
        accumulator.close();
        verify(logSink).log(LOG_TEMPLATE_1, ARG_1);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(logSink);
    }
}
