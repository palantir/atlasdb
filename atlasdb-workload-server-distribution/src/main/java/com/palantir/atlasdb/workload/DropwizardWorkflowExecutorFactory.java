/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload;

import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.workflow.WorkflowExecutorFactory;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;

public class DropwizardWorkflowExecutorFactory implements WorkflowExecutorFactory {

    private static final SafeLogger log = SafeLoggerFactory.get(DropwizardWorkflowExecutorFactory.class);

    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();
    private final LifecycleEnvironment lifecycle;

    public DropwizardWorkflowExecutorFactory(LifecycleEnvironment lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public <T> ExecutorService create(int maxThreadCount, Class<T> workflowFactoryClass, String suffix) {
        // We add 1 to the random number to avoid creating an executor with 0 threads, and to include the maxThreadCount
        // as a possible result (given that the bound is exclusive).
        int numberOfThreads = SECURE_RANDOM.nextInt(maxThreadCount) + 1;
        String executorName = workflowFactoryClass.getSimpleName() + suffix;
        log.info(
                "{} executor created with {} threads",
                SafeArg.of("executorName", executorName),
                SafeArg.of("numberOfThreads", numberOfThreads));
        return lifecycle
                .executorService(executorName)
                .minThreads(numberOfThreads)
                .maxThreads(numberOfThreads)
                .build();
    }
}
