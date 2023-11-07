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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowAndInvariants;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowRunner;
import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AntithesisWorkflowValidatorRunnerTest {

    private static final ListeningExecutorService EXECUTOR_SERVICE =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));

    private WorkflowRunner<Workflow> workflowRunner = new DefaultWorkflowRunner(EXECUTOR_SERVICE);

    @Mock
    private Workflow workflow;

    @Mock
    private WorkflowHistory workflowHistory;

    @Mock
    private InvariantReporter<Void> invariantReporterOne;

    @Mock
    private InvariantReporter<Void> invariantReporterTwo;

    private WorkflowAndInvariants<Workflow> workflowAndInvariants;

    @BeforeEach
    public void before() {
        when(workflow.run()).thenReturn(workflowHistory);
        workflowAndInvariants = WorkflowAndInvariants.of(workflow, invariantReporterOne, invariantReporterTwo);
    }

    @Test
    public void runExecutesWorkflowAndInvokesInvariantReporters() {
        new AntithesisWorkflowValidatorRunner(workflowRunner).run(workflowAndInvariants);
        verify(workflow, times(1)).run();
        verify(invariantReporterOne, times(1)).report(any());
        verify(invariantReporterTwo, times(1)).report(any());
    }

    @Test
    public void runValidatesAllInvariantsIgnoringExceptions() {
        doThrow(new RuntimeException()).when(invariantReporterOne).report(any());
        doThrow(new RuntimeException()).when(invariantReporterTwo).report(any());
        new AntithesisWorkflowValidatorRunner(workflowRunner).run(workflowAndInvariants);
        verify(workflow, times(1)).run();
        verify(invariantReporterOne, times(1)).report(any());
        verify(invariantReporterTwo, times(1)).report(any());
    }

    @Test
    public void runExecutesMultipleWorkflowsAndWaitsForAllToFinishBeforeInvokingInvariantReporter() {
        CountDownLatch slowWorkflowCanMakeProgress = new CountDownLatch(1);
        AtomicBoolean slowWorkflowIsDone = new AtomicBoolean(false);

        Workflow slowWorkflow = mock(Workflow.class);
        when(slowWorkflow.run()).thenAnswer(_input -> {
            try {
                slowWorkflowCanMakeProgress.await();
                slowWorkflowIsDone.set(true);
                return workflowHistory;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        doAnswer(_invocation -> {
                    assertThat(slowWorkflowIsDone.get())
                            .as("the invariant reporter ran, even though the slow workflow was not done yet")
                            .isTrue();
                    return null;
                })
                .when(invariantReporterOne)
                .report(any());

        ExecutorService backgroundExecutor = PTExecutors.newSingleThreadExecutor();
        try {
            Future<Void> validation = backgroundExecutor.submit(() -> {
                WorkflowAndInvariants<Workflow> slowWorkflowAndInvariants =
                        WorkflowAndInvariants.of(slowWorkflow, invariantReporterOne);
                new AntithesisWorkflowValidatorRunner(workflowRunner)
                        .run(slowWorkflowAndInvariants, workflowAndInvariants);
                return null;
            });

            assertThat(validation).as("the slow workflow was not actually slow").isNotDone();

            slowWorkflowCanMakeProgress.countDown();
            Futures.getUnchecked(validation);
        } finally {
            backgroundExecutor.shutdown();
        }

        verify(workflow, times(1)).run();
        verify(slowWorkflow, times(1)).run();
        verify(invariantReporterOne, times(2)).report(any());
        verify(invariantReporterTwo, times(1)).report(any());
    }
}
