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
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;

public class LockCorrectnessCheckerTest {
    private static final int process1 = 1;
    private static final int process2 = 2;
    private static final int process3 = 3;

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runLockCorrectnessChecker(ImmutableList.<Event>of());
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldSucceedWhenNoRefreshes() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.invokeLock(1, process2))
                .add(TestEventUtils.lockSuccess(2, process2))
                .add(TestEventUtils.lockSuccess(3, process1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void allowSimultaneousLockAndRefresh() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(0, process1))
                .add(TestEventUtils.invokeRefresh(0, process1))
                .add(TestEventUtils.refreshSuccess(0, process1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void cannotRefreshWhenAnotherProcessHasLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(3, process2))
                .add(TestEventUtils.invokeRefresh(4, process1))
                .add(TestEventUtils.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(2), eventList.get(3));
    }

    @Test
    public void cannotUnlockWhenAnotherProcessHasLock() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(3, process2))
                .add(TestEventUtils.invokeUnlock(4, process1))
                .add(TestEventUtils.unlockSuccess(5, process1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(eventList.get(2), eventList.get(3));
    }

    @Test
    public void failedLockIntervalCoveredByAnotherProcessSucceeds() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockFailure(3, process2))
                .add(TestEventUtils.invokeRefresh(4, process1))
                .add(TestEventUtils.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldSucceedWhenThereIsASmallWindow() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.invokeLock(2, process3))
                .add(TestEventUtils.invokeRefresh(3, process1))
                .add(TestEventUtils.refreshSuccess(3, process1))
                .add(TestEventUtils.lockSuccess(3, process3))
                .add(TestEventUtils.lockSuccess(4, process2))
                .add(TestEventUtils.invokeRefresh(5, process3))
                .add(TestEventUtils.refreshSuccess(6, process3))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldSucceedForIntervalEdgeCases() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(1, process2))
                .add(TestEventUtils.lockSuccess(2, process2))
                .add(TestEventUtils.invokeLock(3, process3))
                .add(TestEventUtils.lockSuccess(4, process3))
                .add(TestEventUtils.invokeRefresh(4, process1))
                .add(TestEventUtils.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    private static CheckerResult runLockCorrectnessChecker(ImmutableList<Event> events) {
        LockCorrectnessChecker lockCorrectnessChecker = new LockCorrectnessChecker();
        return lockCorrectnessChecker.check(events);
    }
}
