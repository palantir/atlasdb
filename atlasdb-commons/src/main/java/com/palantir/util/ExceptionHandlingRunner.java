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
import java.util.Optional;
import java.util.function.Supplier;

import com.palantir.logsafe.exceptions.SafeRuntimeException;

/**
 * Some more java doc. Blah
 */
public final class ExceptionHandlingRunner implements AutoCloseable {
    private final List<Throwable> failures = new ArrayList<>();

    public ExceptionHandlingRunner() {}

    public ExceptionHandlingRunner(Throwable t) {
        failures.add(t);
    }

    public void runSafely(Runnable shutdownCallback) {
        try {
            shutdownCallback.run();
        } catch (Throwable throwable) {
            failures.add(throwable);
        }
    }

    public <T> Optional<T> supplySafely(Supplier<T> shutdownCallback) {
        try {
            return Optional.of(shutdownCallback.get());
        } catch (Throwable throwable) {
            failures.add(throwable);
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        if (!failures.isEmpty()) {
            RuntimeException closeFailed = new SafeRuntimeException(
                    "Close failed. Please inspect the code and fix the failures");
            failures.forEach(closeFailed::addSuppressed);
            throw closeFailed;
        }
    }
}