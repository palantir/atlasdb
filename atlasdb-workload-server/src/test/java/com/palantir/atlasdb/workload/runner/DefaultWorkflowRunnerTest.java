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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import one.util.streamex.EntryStream;
import org.junit.jupiter.api.Test;

public class DefaultWorkflowRunnerTest {

    @Test
    public void runExecutesAllWorkflows() {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));
        try {
            List<Workflow> workflows = List.of(mock(Workflow.class), mock(Workflow.class));
            DefaultWorkflowRunner runner = new DefaultWorkflowRunner(executor);
            runner.run(workflows).values().forEach(Futures::getUnchecked);
            executor.shutdown();
            workflows.forEach(workflow -> verify(workflow).run());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void runMapsHistoryToTheCorrectWorkflow() {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));
        try {
            Map<Workflow, WorkflowHistory> workflows = Map.of(
                    mock(Workflow.class),
                    mock(WorkflowHistory.class, "history-1"),
                    mock(Workflow.class),
                    mock(WorkflowHistory.class, "history-2"));
            workflows.forEach((key, value) -> when(key.run()).thenReturn(value));
            DefaultWorkflowRunner runner = new DefaultWorkflowRunner(executor);
            Map<Workflow, ListenableFuture<WorkflowHistory>> histories = runner.run(List.copyOf(workflows.keySet()));
            executor.shutdown();
            Map<Workflow, WorkflowHistory> results =
                    EntryStream.of(histories).mapValues(Futures::getUnchecked).toMap();
            assertThat(results).hasSameSizeAs(workflows).containsExactlyInAnyOrderEntriesOf(workflows);
        } finally {
            executor.shutdownNow();
        }
    }
}
