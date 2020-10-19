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
package com.palantir.atlasdb.jepsen;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.utils.CheckerTestUtils;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import org.junit.Test;

public class NemesisResilienceCheckerTest {
    private static final long ZERO_TIME = 0L;
    private static final int PROCESS_1 = 1;
    private static final int PROCESS_2 = 2;

    private static final int IMPOSTOR_PROCESS = -2;
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";

    private static final Event INVOKE_1 = TestEventUtils.invokeTimestamp(ZERO_TIME, PROCESS_1);
    private static final Event INVOKE_2 = TestEventUtils.invokeTimestamp(ZERO_TIME, PROCESS_2);

    private static final Event OK_1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_1, "0");
    private static final Event OK_2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_2, "0");

    private static final Event ERROR_1 = TestEventUtils.createFailEvent(ZERO_TIME, PROCESS_1, "timeout");

    private static final Event NEMESIS_START = TestEventUtils
            .createInfoEvent(ZERO_TIME, JepsenConstants.NEMESIS_PROCESS, JepsenConstants.START_FUNCTION, VALUE_1);

    private static final Event NEMESIS_START_2 = TestEventUtils
            .createInfoEvent(ZERO_TIME, JepsenConstants.NEMESIS_PROCESS, JepsenConstants.START_FUNCTION, VALUE_2);

    private static final Event NEMESIS_STOP = TestEventUtils
            .createInfoEvent(ZERO_TIME, JepsenConstants.NEMESIS_PROCESS, JepsenConstants.STOP_FUNCTION, VALUE_1);

    private static final Event NEMESIS_STOP_2 = TestEventUtils
            .createInfoEvent(ZERO_TIME, JepsenConstants.NEMESIS_PROCESS, JepsenConstants.STOP_FUNCTION, VALUE_2);

    private static final Event IMPOSTOR_START = TestEventUtils
            .createInfoEvent(ZERO_TIME, IMPOSTOR_PROCESS, JepsenConstants.START_FUNCTION);

    private static final Event IMPOSTOR_STOP = TestEventUtils
            .createInfoEvent(ZERO_TIME, IMPOSTOR_PROCESS, JepsenConstants.STOP_FUNCTION);

    @Test
    public void succeedsWithNoEvents() {
        assertNoErrors();
    }

    @Test
    public void succeedsWithNoNemesisAction() {
        assertNoErrors(INVOKE_1, OK_1);
    }

    @Test
    public void succeedsWithNemesisStartWithoutStop() {
        assertNoErrors(OK_1, NEMESIS_START);
    }

    @Test
    public void succeedsWithNemesisStopWithoutStart() {
        assertNoErrors(OK_1, NEMESIS_STOP);
    }

    @Test
    public void succeedsWithInvokeOkBetweenNemesisStartStop() {
        assertNoErrors(NEMESIS_START, INVOKE_1, OK_1, NEMESIS_STOP);
    }

    @Test
    public void succeedsWithOneProcessSuccessfulCycle() {
        assertNoErrors(NEMESIS_START, INVOKE_1, INVOKE_2, OK_1, NEMESIS_STOP);
    }

    @Test
    public void succeedsWithAnySuccessfulInvokeOkCycle() {
        assertNoErrors(NEMESIS_START, INVOKE_1, INVOKE_1, ERROR_1, OK_1, NEMESIS_STOP);
    }

    @Test
    public void succeedsWithNoCycleInStopStartWindow() {
        assertNoErrors(NEMESIS_STOP, NEMESIS_START);
    }

    @Test
    public void succeedsWithMultipleInvokeOksBetweenNemesisStartStop() {
        assertNoErrors(NEMESIS_START, INVOKE_1, OK_1, INVOKE_2, OK_2, NEMESIS_STOP);
    }

    @Test
    public void failsWithConsecutiveNemesisStartStop() {
        assertSimpleNemesisError(NEMESIS_START, NEMESIS_STOP);
    }

    @Test
    public void failsWithInvokeBeforeNemesisStart() {
        assertSimpleNemesisError(INVOKE_1, NEMESIS_START, OK_1, NEMESIS_STOP);
    }

    @Test
    public void failsWithOkAfterNemesisStop() {
        assertSimpleNemesisError(NEMESIS_START, INVOKE_1, NEMESIS_STOP, OK_1);
    }

    @Test
    public void failsWithCycleNotInNemesisWindow() {
        assertSimpleNemesisError(INVOKE_1, NEMESIS_START, OK_1, INVOKE_1, NEMESIS_STOP, OK_1);
    }

    @Test
    public void failsWithUnsuccessfulResponse() {
        assertSimpleNemesisError(NEMESIS_START, INVOKE_1, ERROR_1, NEMESIS_STOP);
    }

    @Test
    public void doesNotObserveCycleWithDifferentProcesses() {
        assertSimpleNemesisError(NEMESIS_START, INVOKE_1, OK_2, NEMESIS_STOP);
    }

    @Test
    public void reportsInnerEventsAsOffending() {
        CheckerResult result = runNemesisResilienceChecker(
                NEMESIS_START,
                NEMESIS_START_2,
                NEMESIS_STOP,
                NEMESIS_STOP_2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START_2, NEMESIS_STOP);
    }

    @Test
    public void succeedsIfCycleBetweenInnermostEvents() {
        assertNoErrors(NEMESIS_START, NEMESIS_START_2, INVOKE_1, OK_1, NEMESIS_STOP, NEMESIS_STOP_2);
    }

    @Test
    public void failsIfCycleNotBetweenInnermostEvents() {
        CheckerResult result = runNemesisResilienceChecker(
                NEMESIS_START,
                INVOKE_1,
                NEMESIS_START_2,
                OK_1,
                INVOKE_2,
                NEMESIS_STOP,
                OK_2,
                NEMESIS_STOP_2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START_2, NEMESIS_STOP);
    }

    @Test
    public void reportsMultipleOffendingNemesisEvents() {
        CheckerResult result = runNemesisResilienceChecker(
                NEMESIS_START,
                NEMESIS_STOP,
                NEMESIS_START_2,
                NEMESIS_STOP_2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START, NEMESIS_STOP, NEMESIS_START_2, NEMESIS_STOP_2);
    }

    @Test
    public void onlyReportsRelevantOffendingEvents() {
        CheckerResult result = runNemesisResilienceChecker(
                NEMESIS_START,
                INVOKE_1,
                OK_1,
                NEMESIS_STOP,
                NEMESIS_START_2,
                NEMESIS_STOP_2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START_2, NEMESIS_STOP_2);
    }

    @Test
    public void failsOnDistributedCycle() {
        CheckerResult result = runNemesisResilienceChecker(
                NEMESIS_START,
                INVOKE_1,
                NEMESIS_STOP,
                NEMESIS_START_2,
                OK_1,
                NEMESIS_STOP_2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START, NEMESIS_STOP, NEMESIS_START_2, NEMESIS_STOP_2);
    }

    @Test
    public void ignoresNonNemesisRelatedInfoEvents() {
        assertNoErrors(IMPOSTOR_START, IMPOSTOR_STOP);
    }

    @Test
    public void ignoresNonNemesisRelatedInfoEventsMidstream() {
        assertNoErrors(NEMESIS_START, INVOKE_1, IMPOSTOR_START, IMPOSTOR_STOP, OK_1, NEMESIS_STOP);
    }

    @Test
    public void skipsNonNemesisRelatedInfoEvents() {
        assertSimpleNemesisError(INVOKE_1, NEMESIS_START, IMPOSTOR_START, OK_1, NEMESIS_STOP, IMPOSTOR_STOP);
    }

    private static void assertNoErrors(Event... events) {
        CheckerTestUtils.assertNoErrors(NemesisResilienceChecker::new, events);
    }

    private static void assertSimpleNemesisError(Event... events) {
        CheckerResult result = runNemesisResilienceChecker(events);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START, NEMESIS_STOP);
    }

    private static CheckerResult runNemesisResilienceChecker(Event... events) {
        NemesisResilienceChecker nemesisResilienceChecker = new NemesisResilienceChecker();
        return nemesisResilienceChecker.check(ImmutableList.copyOf(events));
    }
}
