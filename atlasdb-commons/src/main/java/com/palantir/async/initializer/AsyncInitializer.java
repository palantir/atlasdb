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

package com.palantir.async.initializer;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.logsafe.SafeArg;

@ThreadSafe
public interface AsyncInitializer {
    Logger log = LoggerFactory.getLogger(AsyncInitializer.class);

    default void initialize(boolean initializeAsync) {
        if (initializeAsync) {
            asyncInitialize();
        }
        else {
            tryInitialize();
        }
    }

    default void asyncInitialize() {
        try {
            tryInitialize();
        } catch (Throwable th) {
            cleanUpOnInitFailure();
            log.warn("Failed to initialize {} in the first attempt, will initialize asynchronously.",
                    SafeArg.of("className", this.getClass().getName()), th);

            Executors.newSingleThreadExecutor().execute(
                    () -> {
                        while (!isInitialized()) {
                            try {
                                tryInitialize();
                            } catch (Throwable throwable) {
                                cleanUpOnInitFailure();

                                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
                            }
                        }
                        log.warn("Initialized {} asynchronously.", SafeArg.of("className", this.getClass().getName()));
                    }
            );
        }
    }

    default void checkInitialize() {
        Preconditions.checkArgument(isInitialized(), String.format("The instance of %s is not initialized yet.",
                this.getClass().getName()));
    }

    boolean isInitialized();

    void tryInitialize();

    void cleanUpOnInitFailure();
}
