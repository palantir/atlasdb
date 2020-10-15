/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class DefaultTaskExecutors {
    private static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(5);
    private static final int SINGLE_THREAD = 1;

    @VisibleForTesting
    static final int DEFAULT_QUEUE_CAPACITY = 50_000;

    private DefaultTaskExecutors() {
        // factory
    }

    static ExecutorService createDefaultDeleteExecutor() {
        return PTExecutors.newThreadPoolExecutor(
                SINGLE_THREAD,
                SINGLE_THREAD,
                DEFAULT_IDLE_TIMEOUT.toMillis(),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY),
                new NamedThreadFactory("atlas-delete-executor", true),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
