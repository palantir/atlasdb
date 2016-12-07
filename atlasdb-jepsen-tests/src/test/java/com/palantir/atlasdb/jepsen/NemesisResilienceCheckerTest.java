/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.jepsen;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.ImmutableInfoEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInvokeEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;

public class NemesisResilienceCheckerTest {
    private static final long ZERO_TIME = 0L;
    private static final int PROCESS_0 = 0;
    private static final String PROCESS_NEMESIS = "nemesis";
    private static final String START = "start";
    private static final String STOP = "stop";

    private static final Event INVOKE_0 = ImmutableInvokeEvent.builder()
            .time(ZERO_TIME)
            .process(PROCESS_0)
            .build();
    private static final Event OK_0 = ImmutableOkEvent.builder()
            .time(ZERO_TIME)
            .process(PROCESS_0)
            .value(0L)
            .build();
    private static final Event NEMESIS_START = ImmutableInfoEvent.builder()
            .time(ZERO_TIME)
            .process(PROCESS_NEMESIS)
            .function(START)
            .build();
    private static final Event NEMESIS_STOP = ImmutableInfoEvent.builder()
            .time(ZERO_TIME)
            .process(PROCESS_NEMESIS)
            .function(STOP)
            .build();

    @Test
    public void succeedsWithNoEvents() {
        assertNoErrors();
    }

    @Test
    public void succeedsWithNoNemesisAction() {
        assertNoErrors(INVOKE_0, OK_0);
    }

    @Test
    public void succeedsWithNemesisStartWithoutStop() {
        assertNoErrors(OK_0, NEMESIS_START);
    }

    @Test
    public void succeedsWithNemesisStopWithoutStart() {
        assertNoErrors(OK_0, NEMESIS_STOP);
    }

    @Test
    public void succeedsWithInvokeOkBetweenNemesisStartStop() {
        assertNoErrors(NEMESIS_START, INVOKE_0, OK_0, NEMESIS_STOP);
    }

    @Test
    public void failsWithConsecutiveNemesisStartStop() {
        CheckerResult result = runNemesisResilienceChecker(NEMESIS_START, NEMESIS_STOP);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(NEMESIS_START, NEMESIS_STOP);
    }

    private static void assertNoErrors(Event... events) {
        CheckerResult result = runNemesisResilienceChecker(events);

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    private static CheckerResult runNemesisResilienceChecker(Event... events) {
        NemesisResilienceChecker nemesisResilienceChecker = new NemesisResilienceChecker();
        return nemesisResilienceChecker.check(ImmutableList.copyOf(events));
    }
}
