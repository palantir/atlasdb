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
package com.palantir.atlasdb.jepsen.events;

public abstract class TestEventUtil {

    private static final String LOCKNAME = "default_lockname";

    public static ImmutableInvokeEvent invokeLock(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.LOCK, lockname);
    }

    public static ImmutableInvokeEvent invokeLock(long time, int process) {
        return invokeLock(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent lockSuccess(long time, int process, String lockname) {
        return createOkEvent(time, process, OkEvent.SUCCESS, RequestType.LOCK, lockname);
    }

    public static ImmutableOkEvent lockSuccess(long time, int process) {

        return lockSuccess(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent lockFailure(long time, int process, String lockname) {
        return createOkEvent(time, process, OkEvent.FAILURE, RequestType.LOCK, lockname);
    }

    public static ImmutableOkEvent lockFailure(long time, int process) {

        return lockFailure(time, process, LOCKNAME);
    }

    public static ImmutableInvokeEvent invokeRefresh(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.REFRESH, lockname);
    }

    public static ImmutableInvokeEvent invokeRefresh(long time, int process) {
        return invokeRefresh(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent refreshSuccess(long time, int process, String lockname) {
        return createOkEvent(time, process, OkEvent.SUCCESS, RequestType.REFRESH, lockname);
    }

    public static ImmutableOkEvent refreshSuccess(long time, int process) {
        return refreshSuccess(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent refreshFailure(long time, int process, String lockname) {
        return createOkEvent(time, process, OkEvent.FAILURE, RequestType.REFRESH, lockname);
    }

    public static ImmutableOkEvent refreshFailure(long time, int process) {
        return refreshFailure(time, process, LOCKNAME);
    }

    public static ImmutableInvokeEvent invokeUnlock(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.UNLOCK, lockname);
    }

    public static ImmutableInvokeEvent invokeUnlock(long time, int process) {
        return invokeUnlock(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent unlockSuccess(long time, int process, String lockname) {
        return createOkEvent(time, process, OkEvent.SUCCESS, RequestType.UNLOCK, lockname);
    }

    public static ImmutableOkEvent unlockSuccess(long time, int process) {
        return unlockSuccess(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent unlockFailure(long time, int process, String lockname) {
        return createOkEvent(time, process, OkEvent.FAILURE, RequestType.UNLOCK, lockname);
    }

    public static ImmutableOkEvent unlockFailure(long time, int process) {
        return unlockFailure(time, process, LOCKNAME);
    }

    public static ImmutableInvokeEvent createInvokeEvent(long time, int process, RequestType requestType,
            String resourceName) {
        return ImmutableInvokeEvent.builder()
                .time(time)
                .process(process)
                .requestType(requestType)
                .resourceName(resourceName)
                .build();
    }

    public static ImmutableOkEvent createOkEvent(long time, int process, long value, RequestType requestType,
            String resourceName) {
        return ImmutableOkEvent.builder()
                .time(time)
                .process(process)
                .value(value)
                .requestType(requestType)
                .resourceName(resourceName)
                .build();
    }
}
