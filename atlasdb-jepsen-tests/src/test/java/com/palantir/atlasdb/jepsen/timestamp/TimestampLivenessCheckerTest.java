/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.jepsen.timestamp;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.RequestType;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import org.junit.Test;

public class TimestampLivenessCheckerTest {
    private static final long TIME = 1984L;

    @Test
    public void shouldSucceedIfATimestampWasReturned() {
        assertThat(runTimestampLivenessChecker(TestEventUtils.timestampOk(TIME, 0, "4242424")))
                .satisfies(TimestampLivenessCheckerTest::assertResultValidAndErrorFree);
    }

    @Test
    public void shouldSucceedIfTimestampsWereReturnedAmongOtherEvents() {
        assertThat(runTimestampLivenessChecker(
                TestEventUtils.createFailEvent(TIME, 0),
                TestEventUtils.invokeTimestamp(TIME + 1, 0),
                TestEventUtils.createInfoEvent(TIME + 2, 0, "I come bearing information"),
                TestEventUtils.timestampOk(TIME + 3, 0, "4242424")))
                .satisfies(TimestampLivenessCheckerTest::assertResultValidAndErrorFree);
    }

    @Test
    public void shouldFailIfNoEventsProvided() {
        assertThat(runTimestampLivenessChecker())
                .satisfies(result -> {
                    assertThat(result.valid()).isFalse();
                    assertThat(result.errors()).hasSize(1);
                });
    }

    @Test
    public void shouldFailIfNoOkEventsProvided() {
        assertThat(runTimestampLivenessChecker(
                TestEventUtils.createFailEvent(TIME, 0, "fail"),
                TestEventUtils.invokeTimestamp(TIME + 1, 0),
                TestEventUtils.createInfoEvent(TIME + 2, 0, "abyss"),
                TestEventUtils.createFailEvent(TIME + 3, 0, "crash"),
                TestEventUtils.invokeTimestamp(TIME + 4, 0),
                TestEventUtils.createInfoEvent(TIME + 5, 0, "nadir")))
                .satisfies(result -> {
                    assertThat(result.valid()).isFalse();

                    Event errorEvent = Iterables.getOnlyElement(result.errors());
                    assertThat(errorEvent.time()).isEqualTo(TIME + 5);
                });
    }

    @Test
    public void shouldFailIfOkEventsAreNotTimestamps() {
        assertThat(runTimestampLivenessChecker(
                TestEventUtils.createOkEvent(TIME, 0, "token3141592", RequestType.LOCK),
                TestEventUtils.createOkEvent(TIME + 1, 0, "token3141592", RequestType.REFRESH),
                TestEventUtils.createOkEvent(TIME + 2, 0, "true", RequestType.UNLOCK),
                TestEventUtils.createOkEvent(TIME + 3, 0, "Unserializable?", "curry"),
                TestEventUtils.createOkEvent(TIME + 4, 0, "42", "foldl1")))
                .satisfies(result -> {
                    assertThat(result.valid()).isFalse();

                    Event errorEvent = Iterables.getOnlyElement(result.errors());
                    assertThat(errorEvent.time()).isEqualTo(TIME + 2);
                });
    }

    private static void assertResultValidAndErrorFree(CheckerResult result) {
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    private static CheckerResult runTimestampLivenessChecker(Event... events) {
        TimestampLivenessChecker livenessChecker = new TimestampLivenessChecker();
        return livenessChecker.check(ImmutableList.copyOf(events));
    }
}
