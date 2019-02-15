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

package com.palantir.lock.v2;

import java.time.Duration;

public final class LockLeaseConstants {
    private LockLeaseConstants() {}

    public static final Duration BUFFER = Duration.ofSeconds(2);

    public static final Duration CLIENT_LOCK_REFRESH_PERIOD = Duration.ofSeconds(5);

    public static final Duration SERVER_LEASE_TIMEOUT = Duration.ofSeconds(20);

    /**
     * This value can be changed without requiring any change on client side.
     */
    public static final Duration CLIENT_LEASE_TIMEOUT = SERVER_LEASE_TIMEOUT
            .minus(CLIENT_LOCK_REFRESH_PERIOD)
            .minus(BUFFER);
}
