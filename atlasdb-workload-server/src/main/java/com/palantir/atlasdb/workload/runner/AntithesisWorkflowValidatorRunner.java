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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowHistoryValidator;
import com.palantir.atlasdb.workload.workflow.WorkflowValidator;
import com.palantir.atlasdb.workload.workflow.WorkflowValidatorRunner;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class AntithesisWorkflowValidatorRunner implements WorkflowValidatorRunner<Workflow> {
    private static final SafeLogger log = SafeLoggerFactory.get(AntithesisWorkflowValidatorRunner.class);
    private final ListeningExecutorService listeningExecutorService;

    public AntithesisWorkflowValidatorRunner(ListeningExecutorService listeningExecutorService) {
        this.listeningExecutorService = listeningExecutorService;
    }

    @Override
    public void run(List<WorkflowValidator<Workflow>> workflowValidators) {
        try {
            List<WorkflowHistoryValidator> workflowHistoryValidators = Futures.allAsList(
                            submitWorkflowValidators(workflowValidators))
                    .get();
            log.info("antithesis: stop_faults");
            workflowHistoryValidators.forEach(this::checkAndReportInvariantViolations);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkAndReportInvariantViolations(WorkflowHistoryValidator group) {
        group.invariants().forEach(invariantReporter -> {
            try {
                invariantReporter.report(group.history());
            } catch (RuntimeException e) {
                log.error("Caught an exception when running and reporting an invariant.", e);
            }
        });
        log.info("Dumping transaction log {}", SafeArg.of("transactionLog", group.history()));
    }

    private List<ListenableFuture<WorkflowHistoryValidator>> submitWorkflowValidators(
            List<WorkflowValidator<Workflow>> workflowValidators) {
        return workflowValidators.stream()
                .map(workflowGroup -> listeningExecutorService.submit(workflowGroup::run))
                .collect(Collectors.toList());
    }
}
