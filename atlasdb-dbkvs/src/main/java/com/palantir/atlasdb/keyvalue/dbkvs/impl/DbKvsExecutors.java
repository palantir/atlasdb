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

package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public final class DbKvsExecutors {
    private static final AtomicReference<ExecutorService> SHARED_EXECUTOR = new AtomicReference<>();

    private DbKvsExecutors() {
        // executors
    }

    public static ExecutorService createFixedThreadPool(String threadNamePrefix, DdlConfig config) {
        if (config.sharedPoolSize().isPresent()) {
            createSharedExecutor(threadNamePrefix, config.sharedPoolSize().get());
            return PTExecutors.getViewExecutor(config.poolSize(), Integer.MAX_VALUE, SHARED_EXECUTOR.get());
        }
        return PTExecutors.newFixedThreadPool(config.poolSize(), threadNamePrefix);
    }

    private static synchronized void createSharedExecutor(String threadNamePrefix, int maxPoolSize) {
        if (SHARED_EXECUTOR.get() == null) {
            SHARED_EXECUTOR.set(PTExecutors.newFixedThreadPool(maxPoolSize, threadNamePrefix));
        }
    }
}
