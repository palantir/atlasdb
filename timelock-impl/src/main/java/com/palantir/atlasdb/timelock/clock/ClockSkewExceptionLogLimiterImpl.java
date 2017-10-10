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

import com.google.common.util.concurrent.RateLimiter;

public class ClockSkewExceptionLogLimiterImpl implements ClockSkewExceptionLogLimiter {
    private static final double SECONDS_BETWEEN_EXCEPTION_LOGS = 600; // 10 minutes
    private static final double EXCEPTION_PERMIT_RATE = 1.0 / SECONDS_BETWEEN_EXCEPTION_LOGS;

    private final RateLimiter exceptionLoggingRateLimiter = RateLimiter.create(EXCEPTION_PERMIT_RATE);

    ClockSkewExceptionLogLimiterImpl() {
        // use ClockSkewExceptionLogLimiter.create()
    }

    @Override
    public boolean shouldLogException() {
        return exceptionLoggingRateLimiter.tryAcquire();
    }
}
