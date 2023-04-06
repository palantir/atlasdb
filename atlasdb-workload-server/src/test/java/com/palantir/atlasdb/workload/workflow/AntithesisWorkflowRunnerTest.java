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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.runner.AntithesisWorkflowRunner;
import java.util.List;
import org.junit.Test;

public class AntithesisWorkflowRunnerTest {
    @Test
    public void runExecutesWorkflowsAndInvokesInvariantReporter() {
        Workflow exampleWorkflow = mock(Workflow.class);
        WorkflowHistory workflowHistory = mock(WorkflowHistory.class);
        when(exampleWorkflow.run()).thenReturn(workflowHistory);
        InvariantReporter<Void> exampleInvariantReporterOne = mock(InvariantReporter.class);
        InvariantReporter<Void> exampleInvariantReporterTwo = mock(InvariantReporter.class);
        AntithesisWorkflowRunner.INSTANCE.run(
                exampleWorkflow, List.of(exampleInvariantReporterOne, exampleInvariantReporterTwo));
        verify(exampleWorkflow, times(1)).run();
        verify(exampleInvariantReporterOne, times(1)).report(any());
        verify(exampleInvariantReporterTwo, times(1)).report(any());
    }
}
