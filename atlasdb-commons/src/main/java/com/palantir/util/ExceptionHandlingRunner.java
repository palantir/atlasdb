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

import java.util.ArrayList;
import java.util.List;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeRuntimeException;

/**
 * Runs runnables and suppliers that may throw exceptions, and swallows those exceptions until later. Can be used as a
 * resource in a try block, or delegated to by another resource.
 */
public final class ExceptionHandlingRunner implements AutoCloseable {
    private final List<Throwable> failures = new ArrayList<>();
    private boolean open = true;

    public ExceptionHandlingRunner() {}

    /**
     * Instantiates the runner with a throwable that has already been caught, to be rethrown when close is complete.
     */
    public ExceptionHandlingRunner(Throwable t) {
        failures.add(t);
    }

    public void runSafely(Runnable shutdownCallback) {
        checkOpen();
        try {
            shutdownCallback.run();
        } catch (Throwable throwable) {
            failures.add(throwable);
        }
    }

    private void checkOpen() {
        Preconditions.checkState(open, "exception handling runner is closed.");
    }

    /**
     * Calling close with no failures should be a no-op. Calling close more than once is not supported and will throw
     * a SafeIllegalStateException. This method should only be called as part of a try-with-resources block.
     */
    @Override
    public void close() {
        checkOpen();
        open = false;
        if (!failures.isEmpty()) {
            RuntimeException closeFailed = new SafeRuntimeException(
                    "Close failed. Please inspect the code and fix the failures");
            failures.forEach(closeFailed::addSuppressed);
            throw closeFailed;
        }

    }
}