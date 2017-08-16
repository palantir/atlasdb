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
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Uninterruptibles;

public interface AsyncInitializer {
    Logger log = LoggerFactory.getLogger(AsyncInitializer.class);

    AtomicBoolean isInitialized = new AtomicBoolean(false);

    default void asyncInitialize() {
        Executors.newSingleThreadExecutor().execute(
                () -> {
                    while (!isInitialized.get()) {
                        try {
                            tryInitialize();
                            if (!isInitialized.compareAndSet(false, true)) {
                                log.warn("Someone set initialized for the instance while another thread was initializing it.");
                            };
                            break;
                        } catch (Exception e) {
                            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
                        }
                    }
                }
        );
    }

    default boolean isInitialized() {
        return isInitialized.get();
    }

    void tryInitialize();
}
