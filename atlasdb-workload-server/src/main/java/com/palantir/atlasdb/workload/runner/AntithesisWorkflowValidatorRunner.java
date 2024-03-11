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
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowAndInvariants;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistoryValidator;
import com.palantir.atlasdb.workload.workflow.WorkflowRunner;
import com.palantir.atlasdb.workload.workflow.WorkflowValidatorRunner;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class AntithesisWorkflowValidatorRunner implements WorkflowValidatorRunner<Workflow> {
    private static final SafeLogger log = SafeLoggerFactory.get(AntithesisWorkflowValidatorRunner.class);
    private final WorkflowRunner<Workflow> workflowRunner;

    public AntithesisWorkflowValidatorRunner(WorkflowRunner<Workflow> workflowRunner) {
        this.workflowRunner = workflowRunner;
    }

    // The only reason this receives a supplier, is that we need to start faults before actually selecting the
    // workflows. Otherwise, Antithesis fuzzer cannot appropriately branch before we select the workflows and we'll end
    // up always running the same one, rather than trying all of them.
    @Override
    public void run(Supplier<List<WorkflowAndInvariants<Workflow>>> workflowAndInvariants) {
        try {
            log.info("antithesis: start_faults");
            List<WorkflowHistoryValidator> workflowHistoryValidators = Futures.allAsList(
                            submitWorkflowValidators(workflowAndInvariants.get()))
                    .get();
            log.info("antithesis: stop_faults");
            workflowHistoryValidators.forEach(this::checkAndReportInvariantViolations);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkAndReportInvariantViolations(WorkflowHistoryValidator validator) {
        validator.invariants().forEach(invariantReporter -> {
            try {
                invariantReporter.report(validator.history());
            } catch (RuntimeException e) {
                log.error("Caught an exception when running and reporting an invariant.", e);
            }
        });
        log.info(
                "Dumping transaction log {}",
                SafeArg.of("transactionLog", validator.history().history()));
    }

    private List<ListenableFuture<WorkflowHistoryValidator>> submitWorkflowValidators(
            List<WorkflowAndInvariants<Workflow>> workflowAndInvariants) {
        Map<Workflow, List<InvariantReporter<?>>> reporters = workflowAndInvariants.stream()
                .collect(Collectors.toMap(WorkflowAndInvariants::workflow, WorkflowAndInvariants::invariantReporters));
        Map<Workflow, ListenableFuture<WorkflowHistory>> workflowToHistory =
                workflowRunner.run(workflowAndInvariants.stream()
                        .map(WorkflowAndInvariants::workflow)
                        .collect(Collectors.toList()));
        return workflowToHistory.entrySet().stream()
                .map(entry -> Futures.transform(
                        entry.getValue(),
                        history -> WorkflowHistoryValidator.of(history, reporters.get(entry.getKey())),
                        MoreExecutors.directExecutor()))
                .collect(Collectors.toList());
    }
}
