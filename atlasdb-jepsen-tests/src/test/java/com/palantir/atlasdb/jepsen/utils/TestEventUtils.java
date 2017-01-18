/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.jepsen.utils;

import com.palantir.atlasdb.jepsen.events.ImmutableFailEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInfoEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInvokeEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;

public abstract class TestEventUtils {

    private static final String LOCKNAME = "default_lockname";
    private static final String TIMESTAMP = "timestamp";

    public static ImmutableInvokeEvent invokeLock(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.LOCK, lockname);
    }

    public static ImmutableInvokeEvent invokeLock(long time, int process) {
        return invokeLock(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent lockSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.LOCK_SUCCESS, RequestType.LOCK);
    }

    public static ImmutableOkEvent lockFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.LOCK_FAILURE, RequestType.LOCK);
    }

    public static ImmutableInvokeEvent invokeRefresh(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.REFRESH, lockname);
    }

    public static ImmutableInvokeEvent invokeRefresh(long time, int process) {
        return invokeRefresh(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent refreshSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.REFRESH_SUCCESS, RequestType.REFRESH);
    }

    public static ImmutableOkEvent refreshFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.REFRESH_FAILURE, RequestType.REFRESH);
    }

    public static ImmutableInvokeEvent invokeUnlock(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.UNLOCK, lockname);
    }

    public static ImmutableInvokeEvent invokeUnlock(long time, int process) {
        return invokeUnlock(time, process, LOCKNAME);
    }

    public static ImmutableOkEvent unlockSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.UNLOCK_SUCCESS, RequestType.UNLOCK);
    }

    public static ImmutableOkEvent unlockFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.UNLOCK_FAILURE, RequestType.UNLOCK);
    }

    public static ImmutableInvokeEvent invokeTimestamp(long time, int process) {
        return createInvokeEvent(time, process, RequestType.TIMESTAMP, TIMESTAMP);
    }

    public static ImmutableOkEvent timestampOk(long time, int process, String value) {
        return createOkEvent(time, process, value, RequestType.TIMESTAMP);
    }

    public static ImmutableInvokeEvent createInvokeEvent(long time, int process, String requestType,
            String resourceName) {
        return ImmutableInvokeEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .value(resourceName)
                .build();
    }

    public static ImmutableOkEvent createOkEvent(long time, int process, String value, String requestType) {
        return ImmutableOkEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .value(value)
                .build();
    }

    public static ImmutableFailEvent createFailEvent(long time, int process) {
        return createFailEvent(time, process, "unknown");
    }

    public static ImmutableFailEvent createFailEvent(long time, int process, String error) {
        return ImmutableFailEvent.builder()
                .time(time)
                .process(process)
                .error(error)
                .build();
    }

    public static ImmutableInfoEvent createInfoEvent(long time, String process, String requestType) {
        return ImmutableInfoEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .build();
    }

    public static ImmutableInfoEvent createInfoEvent(long time, String process, String requestType, String value) {
        return ImmutableInfoEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .value(value)
                .build();
    }
}
