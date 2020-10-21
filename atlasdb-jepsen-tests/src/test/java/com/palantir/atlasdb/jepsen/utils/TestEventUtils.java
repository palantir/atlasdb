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
package com.palantir.atlasdb.jepsen.utils;

import com.palantir.atlasdb.jepsen.events.FailEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableFailEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInfoEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInvokeEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;

public abstract class TestEventUtils {

    private static final String LOCKNAME = "default_lockname";
    private static final String TIMESTAMP = "timestamp";

    public static InvokeEvent invokeLock(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.LOCK, lockname);
    }

    public static InvokeEvent invokeLock(long time, int process) {
        return invokeLock(time, process, LOCKNAME);
    }

    public static OkEvent lockSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.LOCK_SUCCESS, RequestType.LOCK);
    }

    public static OkEvent lockFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.LOCK_FAILURE, RequestType.LOCK);
    }

    public static InvokeEvent invokeRefresh(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.REFRESH, lockname);
    }

    public static InvokeEvent invokeRefresh(long time, int process) {
        return invokeRefresh(time, process, LOCKNAME);
    }

    public static OkEvent refreshSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.REFRESH_SUCCESS, RequestType.REFRESH);
    }

    public static OkEvent refreshFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.REFRESH_FAILURE, RequestType.REFRESH);
    }

    public static InvokeEvent invokeUnlock(long time, int process, String lockname) {
        return createInvokeEvent(time, process, RequestType.UNLOCK, lockname);
    }

    public static InvokeEvent invokeUnlock(long time, int process) {
        return invokeUnlock(time, process, LOCKNAME);
    }

    public static OkEvent unlockSuccess(long time, int process) {
        return createOkEvent(time, process, OkEvent.UNLOCK_SUCCESS, RequestType.UNLOCK);
    }

    public static OkEvent unlockFailure(long time, int process) {
        return createOkEvent(time, process, OkEvent.UNLOCK_FAILURE, RequestType.UNLOCK);
    }

    public static InvokeEvent invokeTimestamp(long time, int process) {
        return createInvokeEvent(time, process, RequestType.TIMESTAMP, TIMESTAMP);
    }

    public static OkEvent timestampOk(long time, int process, String value) {
        return createOkEvent(time, process, value, RequestType.TIMESTAMP);
    }

    public static InvokeEvent createInvokeEvent(long time, int process, String requestType, String resourceName) {
        return ImmutableInvokeEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .value(resourceName)
                .build();
    }

    public static OkEvent createOkEvent(long time, int process, String value, String requestType) {
        return ImmutableOkEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .value(value)
                .build();
    }

    public static FailEvent createFailEvent(long time, int process) {
        return createFailEvent(time, process, "unknown");
    }

    public static FailEvent createFailEvent(long time, int process, String error) {
        return ImmutableFailEvent.builder()
                .time(time)
                .process(process)
                .error(error)
                .build();
    }

    public static InfoEvent createInfoEvent(long time, int process, String requestType) {
        return ImmutableInfoEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .build();
    }

    public static InfoEvent createInfoEvent(long time, int process, String requestType, String value) {
        return ImmutableInfoEvent.builder()
                .time(time)
                .process(process)
                .function(requestType)
                .value(value)
                .build();
    }
}
