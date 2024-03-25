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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.workload.invariant.Invariant;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.atlasdb.workload.workflow.WorkflowAndInvariants;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistoryValidator;
import com.palantir.atlasdb.workload.workflow.WorkflowRunner;
import com.palantir.atlasdb.workload.workflow.WorkflowValidatorRunner;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class AntithesisWorkflowValidatorRunner implements WorkflowValidatorRunner<Workflow> {
    private static final SafeLogger log = SafeLoggerFactory.get(AntithesisWorkflowValidatorRunner.class);
    private static final Duration DEFAULT_VALIDATION_RETRY_INTERVAL = Duration.ofSeconds(5);

    @VisibleForTesting
    static final int MAX_ATTEMPTS = 10;

    private final WorkflowRunner<Workflow> workflowRunner;
    private final Duration validationRetryInterval;

    private AntithesisWorkflowValidatorRunner(
            WorkflowRunner<Workflow> workflowRunner, Duration validationRetryInterval) {
        this.workflowRunner = workflowRunner;
        this.validationRetryInterval = validationRetryInterval;
    }

    public static WorkflowValidatorRunner<Workflow> create(WorkflowRunner<Workflow> workflowRunner) {
        return new AntithesisWorkflowValidatorRunner(workflowRunner, DEFAULT_VALIDATION_RETRY_INTERVAL);
    }

    @VisibleForTesting
    static WorkflowValidatorRunner<Workflow> createForTests(WorkflowRunner<Workflow> workflowRunner) {
        return new AntithesisWorkflowValidatorRunner(workflowRunner, Duration.ZERO);
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
        validator.invariants().forEach(invariantReporter -> validateInvariant(invariantReporter, validator.history()));
        log.info(
                "Dumping transaction log {}",
                SafeArg.of("transactionLog", validator.history().history()));
    }

    private <ViolationsT> void validateInvariant(InvariantReporter<ViolationsT> reporter, WorkflowHistory history) {
        Optional<ViolationsT> violations = getInvariantViolations(reporter.invariant(), history);
        violations.ifPresent(presentViolations -> reporter.consumer().accept(presentViolations));
    }

    private <ViolationsT> Optional<ViolationsT> getInvariantViolations(
            Invariant<ViolationsT> invariant, WorkflowHistory history) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                return Optional.of(invariant.apply(history));
            } catch (RuntimeException e) {
                if (attempt == MAX_ATTEMPTS - 1) {
                    log.error(
                            "Caught an exception when running and reporting an invariant; have retried the maximal"
                                    + " number of times. Will no longer attempt to validate.",
                            SafeArg.of("numAttempts", MAX_ATTEMPTS),
                            e);
                    return Optional.empty();
                } else {
                    log.info(
                            "Caught an exception when running and reporting an invariant. Will sleep and then retry.",
                            SafeArg.of("attemptNumber", attempt),
                            SafeArg.of("maxAttempts", MAX_ATTEMPTS),
                            e);

                    // A simple constant backoff. We don't need exponentiality, as we don't expect this to overload
                    // Cassandra or other dependencies. We also don't need jitter, as we normally only deploy one
                    // instance of the workload (and generally won't deploy more than a few).
                    Uninterruptibles.sleepUninterruptibly(validationRetryInterval.toSeconds(), TimeUnit.SECONDS);
                }
            }
        }
        throw new SafeIllegalStateException("Should not reach here");
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
