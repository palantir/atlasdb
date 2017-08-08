/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.clock;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class ClockSkewComparer {
    @VisibleForTesting
    static final Duration MAX_INTERVAL_SINCE_PREVIOUS_REQUEST = Duration.of(10, ChronoUnit.SECONDS);
    @VisibleForTesting
    static final Duration MAX_REQUEST_DURATION = Duration.of(10, ChronoUnit.MILLIS);

    private String server;
    private ClockSkewEvents events;

    private long minElapsedTime;
    private long maxElapsedTime;
    private long remoteElapsedTime;

    public ClockSkewComparer(String server, ClockSkewEvents events, RequestTime previousRequest,
            RequestTime newRequest) {
        this.server = server;
        this.events = events;

        maxElapsedTime = newRequest.localTimeAtEnd() - previousRequest.localTimeAtStart();
        minElapsedTime = newRequest.localTimeAtStart() - previousRequest.localTimeAtEnd();
        remoteElapsedTime = newRequest.remoteSystemTime() - previousRequest.remoteSystemTime();
    }

    public void compare() {
        Preconditions.checkArgument(maxElapsedTime > 0,
                "A positive maxElapsedTime is expected");
        Preconditions.checkArgument(minElapsedTime > 0,
                "A positive minElapsedTime is expected");
        Preconditions.checkArgument(remoteElapsedTime > 0,
                "A positive remoteElapsedTime is expected");

        if (hasTooMuchTimeElapsedSincePreviousRequest()) {
            events.tooMuchTimeSincePreviousRequest(minElapsedTime);
            return;
        }

        if (requestsTookTooLongToComplete()) {
            events.requestsTookTooLong(minElapsedTime, maxElapsedTime);
            return;
        }

        long skew = getSkew();
        events.clockSkew(server, skew);
    }

    private long getSkew() {
        long skew = 0;

        if (remoteElapsedTime < minElapsedTime) {
            skew = minElapsedTime - remoteElapsedTime;
        } else if (remoteElapsedTime > maxElapsedTime) {
            skew = remoteElapsedTime - maxElapsedTime;
        }

        return skew;
    }

    private boolean requestsTookTooLongToComplete() {
        return maxElapsedTime - minElapsedTime > 2 * MAX_REQUEST_DURATION.toNanos();
    }

    private boolean hasTooMuchTimeElapsedSincePreviousRequest() {
        return minElapsedTime > MAX_INTERVAL_SINCE_PREVIOUS_REQUEST.toNanos();
    }
}
