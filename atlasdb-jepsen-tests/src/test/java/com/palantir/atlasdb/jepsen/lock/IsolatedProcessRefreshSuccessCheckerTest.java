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
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.ImmutableInvokeEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;

/**
 * Created by gmaretic on 10/12/2016.
 */
public class IsolatedProcessRefreshSuccessCheckerTest {
    private final String LOCKNAME = "lock";

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runIsolatedProcessRefreshSuccessChecker();

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void onlyRefreshShouldFail(){
        ImmutableInvokeEvent event1 = invokeRefresh(0, 1);
        ImmutableOkEvent event2 = refreshSuccess(1, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2);
        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event2);
    }

    @Test
    public void lockAndRefreshShouldSucceed(){
        ImmutableInvokeEvent event1 = invokeLock(0, 1);
        ImmutableOkEvent event2 = lockSuccess(1, 1);
        ImmutableInvokeEvent event3 = invokeRefresh(2, 1);
        ImmutableOkEvent event4 = refreshSuccess(3, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void cannotRefreshAfterUnlock(){
        ImmutableInvokeEvent event1 = invokeLock(0, 1);
        ImmutableOkEvent event2 = lockSuccess(1, 1);
        ImmutableInvokeEvent event3 = invokeUnlock(2, 1);
        ImmutableOkEvent event4 = unlockSuccess(3, 1);
        ImmutableInvokeEvent event5 = invokeRefresh(4, 1);
        ImmutableOkEvent event6 = refreshSuccess(5, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2, event3, event4, event5, event6);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event4, event6);
    }

    @Test
    public void failedRefreshInvalidatesLock(){
        ImmutableInvokeEvent event1 = invokeLock(0, 1);
        ImmutableOkEvent event2 = lockSuccess(1, 1);
        ImmutableInvokeEvent event3 = invokeRefresh(2, 1);
        ImmutableOkEvent event4 = refreshFailure(3, 1);
        ImmutableInvokeEvent event5 = invokeRefresh(4, 1);
        ImmutableOkEvent event6 = refreshSuccess(5, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2, event3, event4, event5, event6);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event4, event6);
    }

    @Test
    public void canResetLockAfterUnlock(){
        ImmutableInvokeEvent event1 = invokeLock(0, 1);
        ImmutableOkEvent event2 = lockSuccess(1, 1);
        ImmutableInvokeEvent event3 = invokeUnlock(2, 1);
        ImmutableOkEvent event4 = unlockSuccess(3, 1);
        ImmutableInvokeEvent event5 = invokeLock(4, 1);
        ImmutableOkEvent event6 = lockSuccess(5, 1);
        ImmutableInvokeEvent event7 = invokeRefresh(6, 1);
        ImmutableOkEvent event8 = refreshSuccess(7, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2, event3, event4, event5, event6, event7, event8);

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void cannotUnlockTwice(){
        ImmutableInvokeEvent event1 = invokeLock(0, 1);
        ImmutableOkEvent event2 = lockSuccess(1, 1);
        ImmutableInvokeEvent event3 = invokeUnlock(2, 1);
        ImmutableOkEvent event4 = unlockSuccess(3, 1);
        ImmutableInvokeEvent event5 = invokeUnlock(4, 1);
        ImmutableOkEvent event6 = unlockSuccess(5, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2, event3, event4, event5, event6);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event4, event6);
    }

    @Test
    public void unlockFailureHasNoEffect(){
        ImmutableInvokeEvent event1 = invokeLock(0, 1);
        ImmutableOkEvent event2 = lockSuccess(1, 1);
        ImmutableInvokeEvent event3 = invokeUnlock(2, 1);
        ImmutableOkEvent event4 = unlockFailure(3, 1);
        ImmutableInvokeEvent event5 = invokeUnlock(4, 1);
        ImmutableOkEvent event6 = unlockSuccess(5, 1);

        CheckerResult result = runIsolatedProcessRefreshSuccessChecker(event1, event2, event3, event4, event5, event6);

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }



    private ImmutableInvokeEvent invokeLock(long time, int process) {
        return createInvokeEvent(time, process, RequestType.LOCK, LOCKNAME);
    }

    private ImmutableOkEvent lockSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.SUCCESS, RequestType.LOCK, LOCKNAME);
    }

    private ImmutableInvokeEvent invokeRefresh(long time, int process) {
        return createInvokeEvent(time, process, RequestType.REFRESH, LOCKNAME);
    }

    private ImmutableOkEvent refreshSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.SUCCESS, RequestType.REFRESH, LOCKNAME);
    }

    private ImmutableOkEvent refreshFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.FAILURE, RequestType.REFRESH, LOCKNAME);
    }

    private ImmutableInvokeEvent invokeUnlock(long time, int process) {
        return createInvokeEvent(time, process, RequestType.UNLOCK, LOCKNAME);
    }

    private ImmutableOkEvent unlockSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.SUCCESS, RequestType.UNLOCK, LOCKNAME);
    }

    private ImmutableOkEvent unlockFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.FAILURE, RequestType.UNLOCK, LOCKNAME);
    }

    private ImmutableInvokeEvent createInvokeEvent(long time, int process, RequestType requestType, String resourceName) {
        return ImmutableInvokeEvent.builder()
                .time(time)
                .process(process)
                .requestType(requestType)
                .resourceName(resourceName)
                .build();
    }

    private ImmutableOkEvent createOkEvent(long time, int process, long value, RequestType requestType, String resourceName) {
        return ImmutableOkEvent.builder()
                .time(time)
                .process(process)
                .value(value)
                .requestType(requestType)
                .resourceName(resourceName)
                .build();
    }

    private static CheckerResult runIsolatedProcessRefreshSuccessChecker(Event... events) {
        IsolatedProcessRefreshSuccessChecker isolatedProcessRefreshSuccessChecker = new IsolatedProcessRefreshSuccessChecker();
        return isolatedProcessRefreshSuccessChecker.check(ImmutableList.copyOf(events));
    }
}
