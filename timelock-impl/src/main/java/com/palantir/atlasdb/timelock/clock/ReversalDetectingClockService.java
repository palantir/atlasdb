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
package com.palantir.atlasdb.timelock.clock;

import javax.annotation.concurrent.GuardedBy;

class ReversalDetectingClockService implements ClockService {

    private final ClockSkewEvents events;
    private final ClockService delegate;
    private final String server;

    @GuardedBy("this")
    private IdentifiedSystemTime lastReturnedTime = null;

    ReversalDetectingClockService(ClockService delegate, String server, ClockSkewEvents events) {
        this.delegate = delegate;
        this.server = server;
        this.events = events;
    }

    @Override
    public synchronized IdentifiedSystemTime getSystemTime() {
        IdentifiedSystemTime time = delegate.getSystemTime();
        if (lastReturnedTime != null
                && time.getTimeNanos() < lastReturnedTime.getTimeNanos()
                && time.getSystemId().equals(lastReturnedTime.getSystemId())) {
            events.clockWentBackwards(server, Math.abs(lastReturnedTime.getTimeNanos() - time.getTimeNanos()));
        }
        lastReturnedTime = time;
        return time;
    }

}
