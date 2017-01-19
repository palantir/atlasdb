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
    private static final int PROCESS_1 = 1;
    private static final int PROCESS_2 = 2;
    private static final int PROCESS_3 = 3;

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runLockCorrectnessChecker(ImmutableList.<Event>of());
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldSucceedWhenNoRefreshes() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.invokeLock(1, PROCESS_2))
                .add(TestEventUtils.lockSuccess(2, PROCESS_2))
                .add(TestEventUtils.lockSuccess(3, PROCESS_1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void allowSimultaneousLockAndRefresh() {
        ImmutableList<Event> eventList = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, PROCESS_1))
                .add(TestEventUtils.lockSuccess(0, PROCESS_1))
                .add(TestEventUtils.invokeRefresh(0, PROCESS_1))
                .add(TestEventUtils.refreshSuccess(0, PROCESS_1))
                .build();
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
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
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
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
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

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
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
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
        CheckerResult result = runLockCorrectnessChecker(eventList);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    private static CheckerResult runLockCorrectnessChecker(ImmutableList<Event> events) {
        LockCorrectnessChecker lockCorrectnessChecker = new LockCorrectnessChecker();
        return lockCorrectnessChecker.check(events);
    }
}
