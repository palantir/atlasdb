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
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;
import com.palantir.atlasdb.jepsen.utils.CheckerTestUtils;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class PartitionByInvokeNameCheckerHelperTest {

    private static final int PROCESS_1 = 1;
    private static final int PROCESS_2 = 2;
    private static final int PROCESS_3 = 3;
    private static final String LOCK_1 = "lock1";
    private static final String LOCK_2 = "lock2";

    private static final List<Event> eventList = ImmutableList.<Event>builder()
            .add(TestEventUtils.invokeLock(0, PROCESS_1, LOCK_1))
            .add(TestEventUtils.invokeRefresh(1, PROCESS_2, LOCK_1))
            .add(TestEventUtils.lockSuccess(2, PROCESS_1))
            .add(TestEventUtils.invokeUnlock(3, PROCESS_1, LOCK_2))
            .add(TestEventUtils.createInfoEvent(3, PROCESS_3, RequestType.LOCK))
            .add(TestEventUtils.unlockSuccess(4, PROCESS_1))
            .add(TestEventUtils.refreshFailure(5, PROCESS_2))
            .add(TestEventUtils.invokeLock(6, PROCESS_2, LOCK_2))
            .add(TestEventUtils.lockSuccess(7, PROCESS_2))
            .build();

    private static final List<Event> onlyLock2EventList = ImmutableList.<Event>builder()
            .add(TestEventUtils.invokeUnlock(3, PROCESS_1, LOCK_2))
            .add(TestEventUtils.unlockSuccess(4, PROCESS_1))
            .add(TestEventUtils.invokeLock(6, PROCESS_2, LOCK_2))
            .add(TestEventUtils.lockSuccess(7, PROCESS_2))
            .build();

    private static final CheckerResult validResult = ImmutableCheckerResult.builder()
            .valid(true)
            .errors(new ArrayList<>())
            .build();

    /**
     * This checker always fails, returning the entire list of events as the error list. This is useful to verify how
     * PartitionByInvokeNameCheckerHelper manipulates the list. The value of the valid parameter is set to false purely
     * for consistency reasons (since the list of errors is not empty).
     */
    private static final Checker identityChecker = Mockito.mock(Checker.class);

    static {
        Mockito.when(identityChecker.check(Matchers.anyListOf(Event.class)))
                .then(list -> ImmutableCheckerResult.builder()
                        .valid(false)
                        .errors((List) list.getArguments()[0])
                        .build());
    }

    @Test
    public void universalSuccessCheckerShouldSucceedOnNoEvents() {
        Checker universalChecker = Mockito.mock(Checker.class);
        Mockito.when(universalChecker.check(Matchers.anyList())).thenReturn(validResult);

        assertNoError(() -> universalChecker, ImmutableList.<Event>of());
    }

    @Test
    public void universalSuccessCheckerShouldSucceed() {
        Checker universalChecker = Mockito.mock(Checker.class);
        Mockito.when(universalChecker.check(Matchers.anyList())).thenReturn(validResult);

        assertNoError(() -> universalChecker, eventList);
    }

    @Test
    public void infoEventsAreNotLost() {
        InfoEvent infoEvent = TestEventUtils.createInfoEvent(0, PROCESS_1, RequestType.LOCK);

        CheckerResult checkerResult = runPartitionChecker(() -> identityChecker, ImmutableList.<Event>of(infoEvent));

        assertThat(checkerResult.valid()).isFalse();
        assertThat(checkerResult.errors()).containsExactly(infoEvent);
    }

    @Test
    public void universalFailureCheckerShouldFail() {
        InvokeEvent invokeEvent = TestEventUtils.invokeLock(0, PROCESS_1, LOCK_1);
        OkEvent okEvent = TestEventUtils.lockSuccess(1, PROCESS_1);

        CheckerResult checkerResult =
                runPartitionChecker(() -> identityChecker, ImmutableList.of(invokeEvent, okEvent));

        assertThat(checkerResult.valid()).isFalse();
        assertThat(checkerResult.errors()).containsExactly(invokeEvent, okEvent);
    }

    @Test
    public void partitioningOnlyReordersEvents() {
        CheckerResult checkerResult = runPartitionChecker(() -> identityChecker, eventList);

        assertThat(checkerResult.valid()).isFalse();
        assertThat(checkerResult.errors()).hasSameSizeAs(eventList);
        assertThat(checkerResult.errors()).containsOnlyElementsOf(eventList);
    }

    @Test
    public void partitionsRetainRelativeOrdering() {
        CheckerResult checkerResult = runPartitionChecker(() -> filterChecker(LOCK_2), eventList);

        assertThat(checkerResult.valid()).isFalse();
        assertThat(checkerResult.errors()).containsExactlyElementsOf(onlyLock2EventList);
    }

    @Test
    public void infoEventsAreLockIndependent() {
        InvokeEvent invokeEvent = TestEventUtils.invokeLock(0, PROCESS_1, LOCK_1);
        OkEvent okEvent = TestEventUtils.lockSuccess(1, PROCESS_1);

        List<Event> infoEventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.createInfoEvent(0, PROCESS_1, RequestType.LOCK))
                .add(invokeEvent)
                .add(okEvent)
                .add(TestEventUtils.createInfoEvent(2, PROCESS_1, RequestType.LOCK))
                .build();

        CheckerResult checkerResult = runPartitionChecker(() -> filterChecker(LOCK_1), infoEventList);

        assertThat(checkerResult.valid()).isFalse();
        assertThat(checkerResult.errors()).containsExactlyElementsOf(ImmutableList.of(invokeEvent, okEvent));
    }

    /**
     * This method generates a mocked Checker that returns the entire list of Events if and only if all Events in the
     * list are related to the specified lock name, and there is at least one InvokeEvent.
     * @param lockName The lock name to filter for
     * @return mocked Checker
     */
    private Checker filterChecker(String lockName) {
        Checker mockChecker = Mockito.mock(Checker.class);
        Mockito.when(mockChecker.check(Matchers.anyListOf(Event.class))).then(args -> {
            List<Event> events = (List) args.getArguments()[0];
            return checkLockName(lockName, events);
        });
        return mockChecker;
    }

    private CheckerResult checkLockName(String lockName, List<Event> events) {
        boolean noOtherLock = true;
        boolean atLeastOneInvoke = false;
        for (Event event : events) {
            if (event instanceof InvokeEvent) {
                atLeastOneInvoke = true;
                InvokeEvent invokeEvent = (InvokeEvent) event;
                if (!invokeEvent.value().equals(lockName)) {
                    noOtherLock = false;
                }
            }
        }
        if (noOtherLock && atLeastOneInvoke) {
            return ImmutableCheckerResult.builder().valid(false).errors(events).build();
        } else {
            return ImmutableCheckerResult.builder()
                    .valid(true)
                    .errors(new ArrayList<>())
                    .build();
        }
    }

    private static CheckerResult runPartitionChecker(Supplier<Checker> checker, List<Event> events) {
        PartitionByInvokeNameCheckerHelper partitionByInvokeNameCheckerHelper =
                new PartitionByInvokeNameCheckerHelper(checker);
        return partitionByInvokeNameCheckerHelper.check(events);
    }

    private static void assertNoError(Supplier<Checker> checker, List<Event> events) {
        CheckerTestUtils.assertNoErrors(() -> new PartitionByInvokeNameCheckerHelper(checker), events);
    }
}
