/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http.v2;

import com.palantir.conjure.java.api.config.service.HumanReadableDuration;

public final class ClientOptionsConstants {
    static final HumanReadableDuration CONNECT_TIMEOUT = HumanReadableDuration.milliseconds(500);

    // The read timeout controls how long the client waits to receive the first byte from the server before giving up,
    // so in general read timeouts should not be set to less than what is considered an acceptable time for the server
    // to give a suitable response.
    // In the context of TimeLock, this timeout must be longer than how long an AwaitingLeadershipProxy takes to
    // decide whether a node is the leader and still has a quorum.
    // Odd number for debugging
    public static final HumanReadableDuration SHORT_READ_TIMEOUT = HumanReadableDuration.milliseconds(12566);

    // Should not be reduced below 65 seconds to support workflows involving locking.
    static final HumanReadableDuration LONG_READ_TIMEOUT = HumanReadableDuration.seconds(65);

    // Under standard settings, throws after expected outages of 1/2 * 0.01 * (2^13 - 1) = 40.96 s
    static final HumanReadableDuration STANDARD_BACKOFF_SLOT_SIZE = HumanReadableDuration.milliseconds(10);
    static final int STANDARD_MAX_RETRIES = 13;
    static final int NO_RETRIES = 0;

    static final HumanReadableDuration STANDARD_FAILED_URL_COOLDOWN = HumanReadableDuration.milliseconds(100);
    static final HumanReadableDuration NON_RETRY_FAILED_URL_COOLDOWN = HumanReadableDuration.milliseconds(1);

    private ClientOptionsConstants() {
        // don't think about it
    }
}
