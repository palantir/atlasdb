/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.base.Suppliers;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Wraps a potentially expensive failure-handling computation in a memoizing supplier, so that it is only executed
 * once per fixed duration.
 */
public class RateLimitingFailureHandler implements Runnable {
    private final Supplier<Void> delegate;

    private RateLimitingFailureHandler(Supplier<Void> delegate) {
        this.delegate = delegate;
    }

    public static RateLimitingFailureHandler create(Runnable failureHandler, Duration duration) {
        return new RateLimitingFailureHandler(Suppliers.memoizeWithExpiration(
                () -> {
                    failureHandler.run();
                    return null;
                },
                duration.toNanos(),
                TimeUnit.NANOSECONDS));
    }

    @Override
    public void run() {
        delegate.get();
    }
}
