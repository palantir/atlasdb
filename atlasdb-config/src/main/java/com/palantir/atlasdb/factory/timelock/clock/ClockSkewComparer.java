/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory.timelock.clock;

public class ClockSkewComparer {
    private final String server;
    private final ClockSkewEvents events;

    private final long minElapsedTime;
    private final long maxElapsedTime;
    private final long remoteElapsedTime;
    private final long lastRequestDuration;

    public ClockSkewComparer(String server, ClockSkewEvents events, RequestTime previousRequest,
            RequestTime newRequest) {
        this.server = server;
        this.events = events;

        minElapsedTime = newRequest.localTimeAtStart() - previousRequest.localTimeAtEnd();
        maxElapsedTime = newRequest.localTimeAtEnd() - previousRequest.localTimeAtStart();
        remoteElapsedTime = newRequest.remoteSystemTime() - previousRequest.remoteSystemTime();
        lastRequestDuration = newRequest.localTimeAtEnd() - newRequest.localTimeAtStart();
    }

    public void compare() {
        if (clockHasMovedBackwards()) {
            // The clock not moving forwards is already tracked by the ReversalDetectingClockService, so it is fine
            // to no op here. We don't want to use these values.
            return;
        }

        ClockSkewEvent event = getSkew();
        events.clockSkew(server, event, lastRequestDuration);
    }

    private ClockSkewEvent getSkew() {
        return ImmutableClockSkewEvent.builder()
                .maxElapsedTime(maxElapsedTime)
                .minElapsedTime(minElapsedTime)
                .remoteElapsedTime(remoteElapsedTime)
                .build();
    }

    private boolean clockHasMovedBackwards() {
        return minElapsedTime < 0 || maxElapsedTime < 0 || remoteElapsedTime < 0;
    }
}
