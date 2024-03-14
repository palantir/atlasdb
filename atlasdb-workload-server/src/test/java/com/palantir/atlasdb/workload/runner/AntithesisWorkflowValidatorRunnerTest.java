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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.invariant.ImmutableInvariantReporter;
import com.palantir.atlasdb.workload.invariant.Invariant;
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
import java.util.function.Consumer;
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
    private Invariant<String> invariantOne;

    @Mock
    private Invariant<String> invariantTwo;

    @Mock
    private Consumer<String> reporterOne;

    @Mock
    private Consumer<String> reporterTwo;

    private WorkflowAndInvariants<Workflow> workflowAndInvariants;

    @BeforeEach
    public void before() {
        when(workflow.run()).thenReturn(workflowHistory);
        workflowAndInvariants = WorkflowAndInvariants.of(
                workflow,
                ImmutableInvariantReporter.of(invariantOne, reporterOne),
                ImmutableInvariantReporter.of(invariantTwo, reporterTwo));
    }

    @Test
    public void runExecutesWorkflowAndInvokesInvariantReporters() {
        when(invariantOne.apply(any())).thenReturn("all good");
        when(invariantTwo.apply(any())).thenReturn("+1");

        AntithesisWorkflowValidatorRunner.createForTests(workflowRunner).run(workflowAndInvariants);
        verify(workflow, times(1)).run();
        verify(invariantOne, times(1)).apply(any());
        verify(invariantTwo, times(1)).apply(any());
        verify(reporterOne, times(1)).accept(any());
        verify(reporterTwo, times(1)).accept(any());
    }

    @Test
    public void runValidatesAllInvariantsRetryingOnExceptions() {
        doThrow(new RuntimeException())
                .doAnswer(invocation -> "all good")
                .when(invariantOne)
                .apply(any());
        doThrow(new RuntimeException())
                .doThrow(new RuntimeException())
                .doAnswer(invocation -> "+1")
                .when(invariantTwo)
                .apply(any());
        AntithesisWorkflowValidatorRunner.createForTests(workflowRunner).run(workflowAndInvariants);
        verify(workflow, times(1)).run();
        verify(invariantOne, times(2)).apply(any());
        verify(invariantTwo, times(3)).apply(any());
        verify(reporterOne, times(1)).accept(any());
        verify(reporterTwo, times(1)).accept(any());
    }

    @Test
    public void runRetriesInvariantsUpToRetryLimit() {
        doThrow(new RuntimeException()).when(invariantOne).apply(any());
        doThrow(new RuntimeException()).when(invariantTwo).apply(any());
        AntithesisWorkflowValidatorRunner.createForTests(workflowRunner).run(workflowAndInvariants);
        verify(workflow, times(1)).run();
        verify(invariantOne, times(AntithesisWorkflowValidatorRunner.MAX_ATTEMPTS))
                .apply(any());
        verify(invariantTwo, times(AntithesisWorkflowValidatorRunner.MAX_ATTEMPTS))
                .apply(any());
        verify(reporterOne, never()).accept(any());
        verify(reporterTwo, never()).accept(any());
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
                            .as("the invariant was checked, even though the slow workflow was not done yet")
                            .isTrue();
                    return "sehr gut";
                })
                .when(invariantOne)
                .apply(any());
        when(invariantTwo.apply(any())).thenReturn("+1");

        ExecutorService backgroundExecutor = PTExecutors.newSingleThreadExecutor();
        try {
            Future<Void> validation = backgroundExecutor.submit(() -> {
                WorkflowAndInvariants<Workflow> slowWorkflowAndInvariants = WorkflowAndInvariants.of(
                        slowWorkflow, ImmutableInvariantReporter.of(invariantOne, reporterOne));
                AntithesisWorkflowValidatorRunner.createForTests(workflowRunner)
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
        verify(invariantOne, times(2)).apply(any());
        verify(invariantTwo, times(1)).apply(any());
        verify(reporterOne, times(2)).accept(any());
        verify(reporterTwo, times(1)).accept(any());
    }
}
