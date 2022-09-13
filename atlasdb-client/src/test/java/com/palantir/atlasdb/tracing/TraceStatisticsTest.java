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

package com.palantir.atlasdb.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.tracing.Tracer;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class TraceStatisticsTest {
    @Before
    public void before() throws Exception {
        Tracer.initTrace(Optional.of(true), getClass().getSimpleName() + "." + Math.random());
    }

    @Test
    public void trackStats() {
        TraceStatistics.incBytesRead(42);
        TraceStatistics.incEmptyValues(21);

        TraceStatistic traceStatistic = TraceStatistics.getCurrentAndClear();
        assertThat(traceStatistic.bytesReadFromDb()).isEqualTo(42);
        assertThat(traceStatistic.emptyReads()).isEqualTo(21);
    }

    @Test
    public void ignoresIfNoTrace() {
        Tracer.getAndClearTrace();

        TraceStatistics.incBytesRead(42);
        TraceStatistics.incEmptyValues(21);

        TraceStatistic traceStatistic = TraceStatistics.getCurrentAndClear();
        assertThat(traceStatistic.bytesReadFromDb()).isEqualTo(0);
        assertThat(traceStatistic.emptyReads()).isEqualTo(0);
    }

    @Test
    public void trackStats_restores() {
        TraceStatistics.incBytesRead(42);
        TraceStatistic original = TraceStatistics.getCurrentAndClear();

        try {
            TraceStatistics.incBytesRead(23);
        } finally {
            TraceStatistics.getCopyAndRestoreOriginal(original);
        }

        assertThat(TraceStatistics.getCurrentAndClear().bytesReadFromDb()).isEqualTo(42);
    }
}
