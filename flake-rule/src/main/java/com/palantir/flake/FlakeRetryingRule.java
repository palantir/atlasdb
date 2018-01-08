/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.flake;

import java.util.Optional;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class FlakeRetryingRule implements TestRule {
    private static final Logger log = LoggerFactory.getLogger(FlakeRetryingRule.class);

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Optional<ShouldRetry> retryAnnotation = getMostSpecificRetryAnnotation(description);
                if (retryAnnotation.isPresent()) {
                    runStatementWithRetry(retryAnnotation.get(), base, description);
                } else {
                    base.evaluate();
                }
            }
        };
    }

    private static Optional<ShouldRetry> getMostSpecificRetryAnnotation(Description description) {
        Optional<ShouldRetry> methodLevelAnnotation = Optional.ofNullable(description.getAnnotation(ShouldRetry.class));

        return methodLevelAnnotation.isPresent()
                ? methodLevelAnnotation
                : Optional.ofNullable(description.getTestClass().getAnnotation(ShouldRetry.class));
    }

    private static void runStatementWithRetry(ShouldRetry retryAnnotation, Statement base, Description description) {
        Preconditions.checkArgument(retryAnnotation.numAttempts() > 0,
                "Number of attempts should be positive, but found %s", retryAnnotation.numAttempts());

        for (int attempt = 1; attempt <= retryAnnotation.numAttempts(); attempt++) {
            try {
                base.evaluate();
                return;
            } catch (Throwable t) {
                log.info("Test {}.{} failed on attempt {} of {}.",
                        description.getClassName(),
                        description.getMethodName(),
                        attempt,
                        retryAnnotation.numAttempts());
                if (attempt == retryAnnotation.numAttempts()) {
                    throw Throwables.propagate(t);
                }
            }
        }
    }
}
