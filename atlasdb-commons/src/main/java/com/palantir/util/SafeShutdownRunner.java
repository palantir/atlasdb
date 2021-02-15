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

package com.palantir.util;

import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SafeShutdownRunner implements AutoCloseable {
    private final ExecutorService executor = PTExecutors.newCachedThreadPool("safe-shutdown-runner");
    private final List<Throwable> failures = new ArrayList<>();
    private final Duration timeoutDuration;

    public SafeShutdownRunner(Duration timeoutDuration) {
        this.timeoutDuration = timeoutDuration;
    }

    public void shutdownSafely(Runnable shutdownCallback) {
        Future<?> future = executor.submit(shutdownCallback);
        try {
            future.get(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failures.add(e);
        } catch (ExecutionException e) {
            failures.add(e.getCause());
        } catch (TimeoutException e) {
            future.cancel(true);
            failures.add(e);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        if (!failures.isEmpty()) {
            RuntimeException closeFailed =
                    new SafeRuntimeException("Close failed. Please inspect the code and fix the failures");
            failures.forEach(closeFailed::addSuppressed);
            throw closeFailed;
        }
    }
}
