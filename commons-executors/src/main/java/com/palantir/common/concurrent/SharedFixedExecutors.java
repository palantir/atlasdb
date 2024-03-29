/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public final class SharedFixedExecutors {
    private static final Map<String, ExecutorService> SHARED = new ConcurrentHashMap<>();

    private SharedFixedExecutors() {
        // enums should be immutable
    }

    public static ExecutorService getOrCreate(String threadName, int poolSize) {
        return SHARED.computeIfAbsent(threadName, name -> PTExecutors.newFixedThreadPool(poolSize, name));
    }

    public static ExecutorService createOrGetShared(String threadName, int poolSize, Optional<Integer> sharedThreads) {
        if (sharedThreads.isPresent()) {
            ExecutorService sharedExecutor = getOrCreate(threadName, poolSize);
            return PTExecutors.getViewExecutor(threadName, poolSize, Integer.MAX_VALUE, sharedExecutor);
        }
        return PTExecutors.newFixedThreadPool(poolSize, threadName);
    }
}
