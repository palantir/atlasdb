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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.concurrent.NamedThreadFactory;

/**
 * Implements basic infrastructure to allow an object to be asynchronously initialized.
 * In order to be ThreadSafe, the abstract methods of the inheriting class need to be synchronized.
 */
// TODO: Enforce that all methods from this interface are synchronized.
@ThreadSafe
public interface AsyncInitializer {
    Logger log = LoggerFactory.getLogger(AsyncInitializer.class);
    int nThreads = 20;
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(
            nThreads, new NamedThreadFactory("AsyncInitializer", true));

    default void initialize(boolean initializeAsync) {
        synchronized (this) {
            initializeInternal(initializeAsync);
        }
    }

    // TODO (JAVA9): Make this private.
    default void initializeInternal(boolean initializeAsync) {
        if (!initializeAsync) {
            tryToInitializeIfNotInitialized();
            return;
        }

        try {
            tryToInitializeIfNotInitialized();
        } catch (Throwable throwable) {
            log.warn("Failed to initialize " + this.getClass().getName()
                    + " in the first attempt, will initialize asynchronously.", throwable);
            cleanUpOnInitFailure();
            scheduleInitialization();
        }
    }

    // TODO (JAVA9): Make this private.
    default void scheduleInitialization() {
        executorService.schedule(() -> {
            try {
                tryToInitializeIfNotInitialized();
                log.warn("Initialized " +  this.getClass().getName() + " asynchronously.");
            } catch (Throwable throwable) {
                cleanUpOnInitFailure();
                scheduleInitialization();
            }
        }, 10, TimeUnit.SECONDS);
    }

    // TODO (JAVA9): Make this private.
    default void tryToInitializeIfNotInitialized() {
        if (isInitialized()) {
            log.warn(this.getClass().getName() + "was initialized underneath us.");
        } else {
            tryInitialize();
        }
    }

    default void assertNotInitialized() {
        Preconditions.checkState(!isInitialized(),
                "Tried to initialize " + this.getClass().getName() + " , but was already initialized");
    }

    boolean isInitialized();

    /**
     * Tries to initialize the object synchronously.
     * @throws IllegalStateException if object has already been initialized.
     */
    // TODO: Make this method protected abstract.
    void tryInitialize();

    void cleanUpOnInitFailure();
}
