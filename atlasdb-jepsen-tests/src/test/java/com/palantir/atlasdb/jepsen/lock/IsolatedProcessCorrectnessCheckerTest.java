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

public class IsolatedProcessCorrectnessCheckerTest {
    private static final int PROCESS_1 = 1;

    @Test
    public void shouldSucceedOnNoEvents() {
        assertNoError(ImmutableList.<Event>of());
    }

    @Test
    public void onlyLocksSucceeds() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeLock(2, PROCESS_1, "alternate_lock"))
                .add(TestEventUtils.lockFailure(3, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void cannotRefreshWithoutLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeRefresh(0, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1, "alternate_lock"))
                .add(TestEventUtils.refreshFailure(3, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(0), eventList.get(1));
    }

    @Test
    public void cannotRefreshWithWrongLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1, "alternate_lock"))
                .add(TestEventUtils.refreshSuccess(3, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(2), eventList.get(3));
    }

    @Test
    public void cannotUnlockWithoutLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeUnlock(0, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(2, PROCESS_1, "alternate_lock"))
                .add(TestEventUtils.unlockFailure(3, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(0), eventList.get(1));
    }

    @Test
    public void canRefreshAfterLockSuccess() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(3, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void cannotRefreshAfterLockFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockFailure(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(3, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(1), eventList.get(3));
    }

    @Test
    public void cannotRefreshAfterUnlockSuccess() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(2, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(3, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void cannotRefreshAfterUnlockFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(2, PROCESS_1))
                .add(TestEventUtils.unlockFailure(3, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void canRefreshAfterRefreshSuccess() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(3, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void cannotRefreshAfterRefreshFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1))
                .add(TestEventUtils.refreshFailure(3, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(4, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void cannotUnlockAfterRefreshFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(2, PROCESS_1))
                .add(TestEventUtils.refreshFailure(3, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(4, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void cannotUnlockTwice() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(2, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(3, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(4, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(5, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void canResetLockAfterUnlock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeUnlock(2, PROCESS_1))
                .add(TestEventUtils.unlockSuccess(3, PROCESS_1))
                .add(TestEventUtils.invokeLock(4, PROCESS_1))
                .add(TestEventUtils.lockSuccess(5, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(6, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(7, PROCESS_1))
                .build();
        assertNoError(eventList);
    }

    @Test
    public void invalidRefreshResetsState() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeRefresh(0, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(1, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(3, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(4, PROCESS_1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(0), eventList.get(1));
    }

    private static CheckerResult runIsolatedProcessRefreshSuccessChecker(List<Event> events) {
        Checker isolatedProcessCorrectnessChecker =
                new PartitionByInvokeNameCheckerHelper(IsolatedProcessCorrectnessChecker::new);
        return isolatedProcessCorrectnessChecker.check(events);
    }

    private static void assertNoError(List<Event> events) {
        CheckerTestUtils.assertNoErrors(() ->
                new PartitionByInvokeNameCheckerHelper(IsolatedProcessCorrectnessChecker::new), events);
    }
}
