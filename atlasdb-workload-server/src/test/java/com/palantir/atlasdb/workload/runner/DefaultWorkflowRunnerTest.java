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
import com.palantir.common.collect.MapEntries;
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Test;

public class DefaultWorkflowRunnerTest {
    private static final ListeningExecutorService EXECUTOR_SERVICE =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void runExecutesAllWorkflows() {
        List<Workflow> workflows = List.of(mock(Workflow.class), mock(Workflow.class));
        DefaultWorkflowRunner runner = new DefaultWorkflowRunner(EXECUTOR_SERVICE);
        runner.run(workflows).values().forEach(Futures::getUnchecked);
        workflows.forEach(workflow -> verify(workflow).run());
    }

    @Test
    public void runMapsHistoryToTheCorrectWorkflow() {
        Map<Workflow, WorkflowHistory> workflows = Map.of(
                mock(Workflow.class), mock(WorkflowHistory.class), mock(Workflow.class), mock(WorkflowHistory.class));
        workflows.forEach((key, value) -> when(key.run()).thenReturn(value));
        DefaultWorkflowRunner runner = new DefaultWorkflowRunner(EXECUTOR_SERVICE);
        Map<Workflow, ListenableFuture<WorkflowHistory>> histories = runner.run(List.copyOf(workflows.keySet()));

        ListenableFuture<Map<Workflow, WorkflowHistory>> actualWorkflowsFuture = allAsMap(histories);
        assertThat(actualWorkflowsFuture)
                .succeedsWithin(TIMEOUT)
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsExactlyInAnyOrderEntriesOf(workflows);
    }

    private static <K, V> ListenableFuture<Map<K, V>> allAsMap(Map<K, ListenableFuture<V>> keyToFuture) {
        List<ListenableFuture<? extends Entry<K, V>>> entryFutures = keyToFuture.entrySet().stream()
                .map(entry -> Futures.transform(
                        entry.getValue(),
                        value -> new SimpleImmutableEntry<>(entry.getKey(), value),
                        MoreExecutors.directExecutor()))
                .collect(Collectors.toList());

        return Futures.transform(Futures.allAsList(entryFutures), MapEntries::toMap, MoreExecutors.directExecutor());
    }
}
