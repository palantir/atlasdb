/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.workflow;

import java.util.function.Consumer;

public interface Workflow {
    /**
     * Runs desired workflow asynchronously until completion.
     */
    void run();

    /**
     * Registers a callback that will be called synchronously when the workflow has completed.
     * If multiple callbacks are registered, there are no guarantees on the order in which they execute, or that they
     * will execute sequentially or in parallel.
     *
     * Behaviour if consumers are added after a call of {@link #run()} has started are undefined.
     */
    Workflow onComplete(Consumer<WorkflowHistory> consumer);
}
