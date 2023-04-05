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

import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;

public enum AntithesisWorkflowRunner implements WorkflowRunner<Workflow> {
    INSTANCE;

    private static final SafeLogger log = SafeLoggerFactory.get(AntithesisWorkflowRunner.class);

    @Override
    public void run(Workflow workflow, List<InvariantReporter<?>> invariants) {
        WorkflowHistory workflowHistory = workflow.run();
        log.info("antithesis: stop_faults");
        invariants.forEach(reporter -> {
            try {
                reporter.report(workflowHistory);
            } catch (RuntimeException e) {
                log.error("Caught an exception when running and reporting an invariant.", e);
            }
        });
        log.info("Dumping transaction log {}", SafeArg.of("transactionLog", workflowHistory.history()));
    }
}
