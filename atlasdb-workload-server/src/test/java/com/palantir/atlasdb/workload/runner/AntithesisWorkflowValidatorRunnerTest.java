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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowValidator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AntithesisWorkflowValidatorRunnerTest {

    private final ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));

    @Mock
    private Workflow workflow;

    @Mock
    private WorkflowHistory workflowHistory;

    @Mock
    private InvariantReporter<Void> exampleInvariantReporterOne;

    @Mock
    private InvariantReporter<Void> exampleInvariantReporterTwo;

    private WorkflowValidator<Workflow> workflowValidator;

    @Before
    public void before() {
        when(workflow.run()).thenReturn(workflowHistory);
        workflowValidator = WorkflowValidator.builder()
                .workflow(workflow)
                .addInvariants(exampleInvariantReporterOne)
                .addInvariants(exampleInvariantReporterTwo)
                .build();
    }

    @Test
    public void runExecutesWorkflowAndInvokesInvariantReporter() {
        new AntithesisWorkflowValidatorRunner(executorService).run(workflowValidator);
        verify(workflow, times(1)).run();
        verify(exampleInvariantReporterOne, times(1)).report(any());
        verify(exampleInvariantReporterTwo, times(1)).report(any());
    }

    @Test
    public void runExecutesMultipleWorkflowsAndWaitsForAllToFinishBeforeInvokingInvariantReporter() {
        Semaphore semaphore = new Semaphore(0);
        Workflow slowWorkflow = spy(new Workflow() {
            @Override
            public WorkflowHistory run() {
                try {
                    assertThat(semaphore.tryAcquire(5, TimeUnit.SECONDS)).isFalse();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return workflowHistory;
            }
        });

        WorkflowValidator<Workflow> slowWorkflowValidator =
                WorkflowValidator.of(slowWorkflow, List.of(exampleInvariantReporterOne));

        doAnswer(_input -> {
                    semaphore.release();
                    return null;
                })
                .when(exampleInvariantReporterOne)
                .report(any());

        new AntithesisWorkflowValidatorRunner(executorService).run(workflowValidator, slowWorkflowValidator);

        verify(workflow, times(1)).run();
        verify(slowWorkflow, times(1)).run();
        verify(exampleInvariantReporterOne, times(2)).report(any());
        verify(exampleInvariantReporterTwo, times(1)).report(any());
    }
}
