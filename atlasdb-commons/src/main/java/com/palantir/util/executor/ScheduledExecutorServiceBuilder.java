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

package com.palantir.util.executor;

import java.util.concurrent.ScheduledExecutorService;

public interface ScheduledExecutorServiceBuilder {
    ScheduledExecutorServiceBuilder numThreads(int numThreads);
    ScheduledExecutorServiceBuilder daemon(boolean daemon);

    /**
     * This methods sets the argument that decides which trace will be used in each execution of the submitted threads.
     * If called with a value of true, a new trace will be created for every execution of the submitted tasks. This mode
     * is useful when an application has a periodic background task, not triggered by any request.
     *
     * If called with a value of false, the trace of the thread that submitted the task will be used for every execution
     * of it. This mode is useful for tasks that are submitted as the result of a request.
     */
    ScheduledExecutorServiceBuilder withNewTrace(boolean withNewTrace);

    ScheduledExecutorService build();
}
