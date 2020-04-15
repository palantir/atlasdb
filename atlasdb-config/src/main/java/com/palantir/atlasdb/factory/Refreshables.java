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

package com.palantir.atlasdb.factory;


import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.refreshable.DefaultRefreshable;
import com.palantir.refreshable.Refreshable;

final class Refreshables {

    private Refreshables() {}

    /** Creates a refreshable that updates based on the result of invoking the {@link Callable} at the given period. */
    @SuppressWarnings("FutureReturnValueIgnored")
    static <T> Refreshable<T> create(
            Callable<T> callable,
            Consumer<Throwable> exceptionHandler,
            ScheduledExecutorService executor,
            Duration refreshInterval) {
        Preconditions.checkArgument(
                refreshInterval.toNanos() > 0, "Cannot create Refreshable with 0 or negative refresh interval");

        DefaultRefreshable<T> refreshable = new DefaultRefreshable<>(call(callable));

        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshable.update(callable.call());
                    } catch (Throwable e) {
                        exceptionHandler.accept(e);
                    }
                },
                refreshInterval.toNanos(),
                refreshInterval.toNanos(),
                TimeUnit.NANOSECONDS);

        return refreshable;
    }

    static <T> T call(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new SafeRuntimeException("Cannot create Refreshable unless initial value is constructable", e);
        }
    }
}
