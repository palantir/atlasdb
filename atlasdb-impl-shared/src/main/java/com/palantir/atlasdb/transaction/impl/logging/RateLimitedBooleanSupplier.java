/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.logging;

import java.util.function.BooleanSupplier;

import com.google.common.util.concurrent.RateLimiter;

public class RateLimitedBooleanSupplier implements BooleanSupplier {
    private final RateLimiter rateLimiter;

    private RateLimitedBooleanSupplier(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    /**
     * Creates a {@link BooleanSupplier} that returns true at most once every secondsBetweenTrues seconds (in terms
     * of wall-clock time). The first returned value should always be true.
     *
     * @param secondsBetweenTrues number of seconds between 'true' responses.
     * @return boolean supplier following the aforementioned rules.
     */
    public static BooleanSupplier create(double secondsBetweenTrues) {
        if (secondsBetweenTrues == 0.0) {
            return () -> true;
        }
        return new RateLimitedBooleanSupplier(RateLimiter.create(1.0 / secondsBetweenTrues));
    }

    @Override
    public boolean getAsBoolean() {
        return rateLimiter.tryAcquire();
    }
}
