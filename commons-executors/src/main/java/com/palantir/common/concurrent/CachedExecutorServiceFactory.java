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

import java.util.concurrent.ExecutorService;

/**
 * Configurable factory for cached executor instances.
 * Consumers of this library may wish to provide additional configuration
 * around default executor instances.
 */
public interface CachedExecutorServiceFactory {

    /**
     * Gets an unbounded {@link ExecutorService} with daemon threads using the provided thread name prefix and timeout.
     */
    ExecutorService getCachedExecutorService(String threadNamePrefix, int threadIdleTimeoutMillis);

    /**
     * Gets an unbounded {@link ExecutorService} with daemon threads using the provided thread name prefix.
     * Creates an executor using the default idle timeout.
     */
    ExecutorService getCachedExecutorService(String threadNamePrefix);

    /**
     * Get the priority of this factory. The factory service with the highest priority will be used.
     *
     * @return Priority of this factory
     */
    int getPriority();
}
