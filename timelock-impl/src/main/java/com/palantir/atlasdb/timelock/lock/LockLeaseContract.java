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

package com.palantir.atlasdb.timelock.lock;

import java.time.Duration;

public final class LockLeaseContract {
    private LockLeaseContract() {}

    public static final Duration SERVER_LEASE_TIMEOUT = Duration.ofSeconds(1);

    /**
     * This value can be changed without requiring any change on client side.
     *
     * To guarantee correctness CLIENT_LEASE_TIMEOUT should always be less than SERVER_LEASE_TIMEOUT.
     *
     * To guarantee liveness, CLIENT_LEASE_TIMEOUT should be less than SERVER_LEASE_TIMEOUT by client side refresh
     * period. Current value is picked such that client will be able to refresh held locks even after two consecutive
     * failed refresh calls. (Where client side refresh period is 5 seconds)
     */
    public static final Duration CLIENT_LEASE_TIMEOUT = Duration.ofMillis(500);
}
