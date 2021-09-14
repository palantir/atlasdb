/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.flake;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Arrays;
import java.util.Optional;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A {@link TestRule} that retries methods and classes annotated with the {@link ShouldRetry} annotation.
 *
 * Like other JUnit TestRules, these rules are applied around the {@link org.junit.Before}/Test/{@link org.junit.After}
 * sequence. Thus, methods annotated @Before will run before *each* attempt at running a test, and methods annotated
 * with @After will similarly run after each attempt. However, since retrying takes place at the method level,
 * methods annotated with {@link org.junit.BeforeClass} and {@link org.junit.AfterClass} will NOT be re-run.
 *
 * To illustrate this, consider a test class with one flaky method that has been annotated with ShouldRetry.
 * The order in which methods are run is as follows:
 * <pre>
 *     BeforeClass runs
 *     Before runs
 *     Test fails on attempt 1
 *     After runs
 *     Before runs
 *     Test succeeds on attempt 2
 *     After runs
 *     AfterClass runs
 * </pre>
 *
 * Retrying flaky implementations of BeforeClass and AfterClass isn't currently supported, but may be supported
 * in a future release.
 *
 * Note: Please be very careful about ordering when chaining this with other JUnit test rules.
 */
public class FlakeRetryingRule implements TestRule {
    private static final SafeLogger log = SafeLoggerFactory.get(FlakeRetryingRule.class);

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
        Preconditions.checkArgument(
                retryAnnotation.numAttempts() > 0,
                "Number of attempts should be positive, but found %s",
                retryAnnotation.numAttempts());

        for (int attempt = 1; attempt <= retryAnnotation.numAttempts(); attempt++) {
            try {
                base.evaluate();
                log.info(
                        "Test {}.{} succeeded on attempt {} of {}.",
                        SafeArg.of("testClass", description.getClassName()),
                        SafeArg.of("testMethod", description.getMethodName()),
                        SafeArg.of("attempt", attempt),
                        SafeArg.of("maxAttempts", retryAnnotation.numAttempts()));
                return;
            } catch (Throwable t) {
                if (Arrays.stream(retryAnnotation.retryableExceptions()).anyMatch(type -> causeHasType(t, type))) {
                    logFailureAndThrowIfNeeded(retryAnnotation, description, attempt, t);
                } else {
                    throw Throwables.propagate(t);
                }
            }
        }
    }

    private static boolean causeHasType(Throwable cause, Class<? extends Throwable> type) {
        return cause != null && (type.isInstance(cause) || causeHasType(cause.getCause(), type));
    }

    private static void logFailureAndThrowIfNeeded(
            ShouldRetry retryAnnotation, Description description, int attempt, Throwable throwable) {
        log.info(
                "Test {}.{} failed on attempt {} of {}.",
                SafeArg.of("testClass", description.getClassName()),
                SafeArg.of("testMethod", description.getMethodName()),
                SafeArg.of("attempt", attempt),
                SafeArg.of("maxAttempts", retryAnnotation.numAttempts()),
                throwable);
        if (attempt == retryAnnotation.numAttempts()) {
            throw Throwables.propagate(throwable);
        }
    }
}
