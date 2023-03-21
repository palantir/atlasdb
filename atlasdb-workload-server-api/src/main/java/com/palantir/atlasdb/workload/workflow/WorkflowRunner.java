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

import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import java.util.List;

/**
 * Run the provided workflow and execute the provided invariant reporters.
 *
 * The idea of this class is that long-term we will want to have multiple ways to run/execute workflows,
 * rather than biasing towards the present case (just once).
 */
public interface WorkflowRunner<WorkflowTypeT extends Workflow> {
    void run(WorkflowTypeT workflow, List<InvariantReporter<?>> invariantReporters);
}
