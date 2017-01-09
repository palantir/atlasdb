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
package com.palantir.atlasdb.jepsen.lock;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.TestEventUtil;

public class IsolatedProcessCorrectnessCheckerTest {
    private static final int process1 = 1;

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(ImmutableList.<Event>of());
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void onlyLocksSucceeds() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeLock(2, process1, "alternate_lock"))
                .add(TestEventUtil.lockFailure(3, process1, "alternate_lock"))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void cannotRefreshWithoutLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeRefresh(0, process1))
                .add(TestEventUtil.refreshSuccess(1, process1))
                .add(TestEventUtil.invokeRefresh(2, process1, "alternate_lock"))
                .add(TestEventUtil.refreshFailure(3, process1, "alternate_lock"))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(1), eventList.get(1));
    }

    @Test
    public void cannotUnlockWithoutLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeUnlock(0, process1))
                .add(TestEventUtil.unlockSuccess(1, process1))
                .add(TestEventUtil.invokeUnlock(2, process1, "alternate_lock"))
                .add(TestEventUtil.unlockFailure(3, process1, "alternate_lock"))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(1), eventList.get(1));
    }

    @Test
    public void canRefreshAfterLockSuccess() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeRefresh(2, process1))
                .add(TestEventUtil.refreshSuccess(3, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void cannotRefreshAfterLockFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockFailure(1, process1))
                .add(TestEventUtil.invokeRefresh(2, process1))
                .add(TestEventUtil.refreshSuccess(3, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(1), eventList.get(3));
    }

    @Test
    public void cannotRefreshAfterUnlockSuccess() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeUnlock(2, process1))
                .add(TestEventUtil.unlockSuccess(3, process1))
                .add(TestEventUtil.invokeRefresh(4, process1))
                .add(TestEventUtil.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void cannotRefreshAfterUnlockFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeUnlock(2, process1))
                .add(TestEventUtil.unlockFailure(3, process1))
                .add(TestEventUtil.invokeRefresh(4, process1))
                .add(TestEventUtil.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void canRefreshAfterRefreshSuccess() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeRefresh(2, process1))
                .add(TestEventUtil.refreshSuccess(3, process1))
                .add(TestEventUtil.invokeRefresh(4, process1))
                .add(TestEventUtil.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void cannotRefreshAfterRefreshFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeRefresh(2, process1))
                .add(TestEventUtil.refreshFailure(3, process1))
                .add(TestEventUtil.invokeRefresh(4, process1))
                .add(TestEventUtil.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void cannotUnlockAfterRefreshFailure() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeRefresh(2, process1))
                .add(TestEventUtil.refreshFailure(3, process1))
                .add(TestEventUtil.invokeUnlock(4, process1))
                .add(TestEventUtil.unlockSuccess(5, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void cannotUnlockTwice() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeUnlock(2, process1))
                .add(TestEventUtil.unlockSuccess(3, process1))
                .add(TestEventUtil.invokeUnlock(4, process1))
                .add(TestEventUtil.unlockSuccess(5, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(3), eventList.get(5));
    }

    @Test
    public void canResetLockAfterUnlock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtil.invokeLock(0, process1))
                .add(TestEventUtil.lockSuccess(1, process1))
                .add(TestEventUtil.invokeUnlock(2, process1))
                .add(TestEventUtil.unlockSuccess(3, process1))
                .add(TestEventUtil.invokeLock(4, process1))
                .add(TestEventUtil.lockSuccess(5, process1))
                .add(TestEventUtil.invokeRefresh(6, process1))
                .add(TestEventUtil.refreshSuccess(7, process1))
                .build();
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    private static CheckerResult runIsolatedProcessRefreshSuccessChecker(ImmutableList<Event> events) {
        IsolatedProcessCorrectnessChecker isolatedProcessCorrectnessChecker = new IsolatedProcessCorrectnessChecker();
        return isolatedProcessCorrectnessChecker.check(events);
    }
}
