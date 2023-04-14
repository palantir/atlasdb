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
import java.util.Arrays;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface WorkflowValidator<WorkflowTypeT extends Workflow> {
    @Value.Parameter
    WorkflowTypeT workflow();

    @Value.Parameter
    List<InvariantReporter<?>> invariants();

    static <WorkflowTypeT extends Workflow> WorkflowValidator<WorkflowTypeT> of(
            WorkflowTypeT workflow, List<InvariantReporter<?>> invariants) {
        return ImmutableWorkflowValidator.of(workflow, invariants);
    }

    static <WorkflowTypeT extends Workflow> WorkflowValidator<WorkflowTypeT> of(
            WorkflowTypeT workflow, InvariantReporter<?>... invariants) {
        return ImmutableWorkflowValidator.of(workflow, Arrays.asList(invariants));
    }

    static <WorkflowTypeT extends Workflow> ImmutableWorkflowValidator.Builder<WorkflowTypeT> builder() {
        return ImmutableWorkflowValidator.builder();
    }
}
