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

package com.palantir.common.concurrent;

import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

public final class CachedExecutors {
    private static final Logger log = LoggerFactory.getLogger(CachedExecutors.class);

    private static final Supplier<CachedExecutorServiceFactory> executorFactory = Suppliers.memoize(() -> {
        int maxPriority = Integer.MIN_VALUE;
        CachedExecutorServiceFactory highestPriority = null;

        try {
            for (CachedExecutorServiceFactory factory : ServiceLoader.load(CachedExecutorServiceFactory.class)) {
                int priority = factory.getPriority();
                if (priority > maxPriority) {
                    maxPriority = priority;
                    highestPriority = factory;
                }
            }
        } catch (Throwable t) {
            log.error("Failed to search for CachedExecutorServiceFactory instances", t);
        }
        if (highestPriority == null) {
            highestPriority = new PTExecutorsCachedExecutorServiceFactory();
        }
        log.debug("Using CachedExecutorServiceFactory implementation {} with priority {}",
                highestPriority, maxPriority);
        return highestPriority;
    });

    public static ExecutorService getCachedExecutorService(String threadNamePrefix, int threadIdleTimeoutMillis) {
        return executorFactory.get().getCachedExecutorService(threadNamePrefix, threadIdleTimeoutMillis);
    }

    public static ExecutorService getCachedExecutorService(String threadNamePrefix) {
        return executorFactory.get().getCachedExecutorService(threadNamePrefix);
    }

    private CachedExecutors() {
        // Utility
    }

}
