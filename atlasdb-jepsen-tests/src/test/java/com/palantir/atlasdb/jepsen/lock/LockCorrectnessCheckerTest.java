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
package com.palantir.atlasdb.jepsen.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.PartitionByInvokeNameCheckerHelper;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.utils.CheckerTestUtils;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import java.util.List;
import org.junit.Test;

public class LockCorrectnessCheckerTest {
    private static final int PROCESS_1 = 1;
    private static final int PROCESS_2 = 2;
    private static final int PROCESS_3 = 3;

    @Test
    public void shouldSucceedOnNoEvents() {
        assertNoError(ImmutableList.<Event>of());
    }

    @Test
    public void shouldSucceedWhenNoRefreshes() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.invokeLock(1, PROCESS_2))
                .add(TestEventUtils.lockSuccess(2, PROCESS_2))
                .add(TestEventUtils.lockSuccess(3, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void allowSimultaneousLockAndRefresh() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(0, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(0, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(0, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void cannotRefreshWhenAnotherProcessHasLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_2))
                .add(TestEventUtils.lockSuccess(3, PROCESS_2))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(2), eventList.get(3));
    }

    @Test
    public void canRefreshWhenAnotherProcessHasDifferentLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_2, "alternate_lock"))
                .add(TestEventUtils.lockSuccess(3, PROCESS_2))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void cannotRefreshWhileAnotherProcessHasLockWithDoubleRefresh() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_2))
                .add(TestEventUtils.invokeRefresh(3, PROCESS_1))
                .add(TestEventUtils.lockSuccess(4, PROCESS_2))
                .add(TestEventUtils.refreshSuccess(4, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(5, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(6, PROCESS_1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(2), eventList.get(4));
    }

    @Test
    public void cannotRefreshWhileLockHeldByOneOfTwoOtherProcesses() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_2))
                .add(TestEventUtils.invokeLock(1, PROCESS_1))
                .add(TestEventUtils.lockSuccess(2, PROCESS_1))
                .add(TestEventUtils.invokeLock(3, PROCESS_3))
                .add(TestEventUtils.lockSuccess(4, PROCESS_2))
                .add(TestEventUtils.invokeRefresh(5, PROCESS_1))
                .add(TestEventUtils.lockSuccess(6, PROCESS_3))
                .add(TestEventUtils.refreshSuccess(7, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(8, PROCESS_2))
                .add(TestEventUtils.refreshSuccess(9, PROCESS_2))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(6));
    }

    @Test
    public void cannotUnlockWhenAnotherProcessHasLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_2))
                .add(TestEventUtils.lockSuccess(3, PROCESS_2))
                .add(TestEventUtils.invokeUnlock(4, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(2), eventList.get(3));
    }

    @Test
    public void failedLockIntervalCoveredByAnotherProcessSucceeds() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_2))
                .add(TestEventUtils.lockFailure(3, PROCESS_2))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    /**
     * The following test should succeed because it is theoretically possible for PROCESS_1 to hold the lock,
     * refresh it at time 3, then immediately lose the lock, the lock being granted to PROCESS_2 at time 3, then
     * again immediately lose it, the lock being granted to PROCESS_3, still at time 3. Then, a delayed response
     * informs PROCESS_2 that it was granted the lock, but it is actually PROCESS_3 that holds the lock.
     */
    @Test
    public void shouldSucceedWhenThereIsASmallWindow() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_2))
                .add(TestEventUtils.invokeLock(2, PROCESS_3))
                .add(TestEventUtils.invokeRefresh(3, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(3, PROCESS_1))
                .add(TestEventUtils.lockSuccess(3, PROCESS_3))
                .add(TestEventUtils.lockSuccess(4, PROCESS_2))
                .add(TestEventUtils.invokeRefresh(5, PROCESS_3))
                .add(TestEventUtils.refreshSuccess(6, PROCESS_3))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void shouldSucceedForIntervalEdgeCases() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(1, PROCESS_2))
                .add(TestEventUtils.lockSuccess(2, PROCESS_2))
                .add(TestEventUtils.invokeLock(3, PROCESS_3))
                .add(TestEventUtils.lockSuccess(4, PROCESS_3))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    private static CheckerResult runLockCorrectnessChecker(ImmutableList<Event> events) {
        Checker lockCorrectnessChecker = new PartitionByInvokeNameCheckerHelper(LockCorrectnessChecker::new);
        return lockCorrectnessChecker.check(events);
    }

    private static void assertNoError(List<Event> events) {
        CheckerTestUtils.assertNoErrors(
                () -> new PartitionByInvokeNameCheckerHelper(LockCorrectnessChecker::new), events);
    }
}
