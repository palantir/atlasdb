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

public class RefreshCorrectnessCheckerTest {
    private static final int process1 = 1;
    private static final int process2 = 2;

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runRefreshCorrectnessChecker(ImmutableList.<Event>of());
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void simpleNonOverlappingRefreshesShouldSucceed() {
        ImmutableList<Event> list = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeRefresh(2, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(3, process2))
                .add(TestEventUtils.refreshSuccess(3, process1))
                .add(TestEventUtils.invokeRefresh(4, process2))
                .add(TestEventUtils.refreshSuccess(5, process2))
                .build();
        CheckerResult result = runRefreshCorrectnessChecker(list);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void simpleOverlappingRefreshesShouldFail() {
        ImmutableList<Event> list = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(3, process2))
                .add(TestEventUtils.invokeRefresh(4, process1))
                .add(TestEventUtils.invokeRefresh(4, process2))
                .add(TestEventUtils.refreshSuccess(5, process1))
                .add(TestEventUtils.refreshSuccess(5, process2))
                .build();
        CheckerResult result = runRefreshCorrectnessChecker(list);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(list.get(5), list.get(7));
    }

    @Test
    public void successiveRefreshesShouldSucceed() {
        ImmutableList<Event> list = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeRefresh(2, process1))
                .add(TestEventUtils.refreshSuccess(3, process1))
                .add(TestEventUtils.invokeRefresh(4, process1))
                .add(TestEventUtils.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runRefreshCorrectnessChecker(list);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void anotherProcessCannotRefreshBetweenSuccessiveRefreshes() {
        ImmutableList<Event> list = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeRefresh(2, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(2, process2))
                .add(TestEventUtils.invokeRefresh(3, process2))
                .add(TestEventUtils.refreshSuccess(4, process2))
                .add(TestEventUtils.refreshSuccess(4, process1))
                .add(TestEventUtils.invokeRefresh(4, process1))
                .add(TestEventUtils.refreshSuccess(5, process1))
                .build();
        CheckerResult result = runRefreshCorrectnessChecker(list);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(list.get(8), list.get(9));
    }

    @Test
    public void anotherProcessCanGrabLockAtLastRefreshInvoke() {
        ImmutableList<Event> list = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeRefresh(2, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(2, process2))
                .add(TestEventUtils.invokeRefresh(3, process2))
                .add(TestEventUtils.refreshSuccess(4, process2))
                .add(TestEventUtils.refreshSuccess(4, process1))
                .build();
        CheckerResult result = runRefreshCorrectnessChecker(list);
        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void successfulUnlockImpliesHoldingLock() {
        ImmutableList<Event> list = ImmutableList.<Event>builder()
                .add(TestEventUtils.invokeLock(0, process1))
                .add(TestEventUtils.lockSuccess(1, process1))
                .add(TestEventUtils.invokeLock(2, process2))
                .add(TestEventUtils.lockSuccess(3, process2))
                .add(TestEventUtils.invokeUnlock(4, process1))
                .add(TestEventUtils.invokeRefresh(4, process2))
                .add(TestEventUtils.unlockSuccess(5, process1))
                .add(TestEventUtils.refreshSuccess(5, process2))
                .build();
        CheckerResult result = runRefreshCorrectnessChecker(list);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(list.get(5), list.get(7));
    }

    private static CheckerResult runRefreshCorrectnessChecker(ImmutableList<Event> events) {
        RefreshCorrectnessChecker refreshCorrectnessChecker = new RefreshCorrectnessChecker();
        return refreshCorrectnessChecker.check(events);
    }
}
