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

package com.palantir.atlasdb.workload.runner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.Test;

public class DefaultWorkflowRunnerTest {

    private static final ListeningExecutorService EXECUTOR_SERVICE =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));

    @Test
    public void runExecutesAllWorkflows() {
        List<Workflow> workflows = List.of(mock(Workflow.class), mock(Workflow.class));
        DefaultWorkflowRunner runner = new DefaultWorkflowRunner(EXECUTOR_SERVICE);
        runner.run(workflows);
        workflows.forEach(workflow -> verify(workflow).run());
    }

    @Test
    public void runMapsHistoryToTheCorrectWorkflow() {
        Map<Workflow, WorkflowHistory> workflows = Map.of(
                mock(Workflow.class), mock(WorkflowHistory.class), mock(Workflow.class), mock(WorkflowHistory.class));
        workflows.entrySet().forEach(entry -> when(entry.getKey().run()).thenReturn(entry.getValue()));
        DefaultWorkflowRunner runner = new DefaultWorkflowRunner(EXECUTOR_SERVICE);
        Map<> runner.run(List.copyOf(workflows.keySet()));
        workflows.forEach(workflow -> verify(workflow).run());
    }
}
