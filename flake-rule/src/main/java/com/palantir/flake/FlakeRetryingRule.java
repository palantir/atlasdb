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
                log.info("Test {}.{} succeeded on attempt {} of {}.",
                        description.getClassName(),
                        description.getMethodName(),
                        attempt,
                        retryAnnotation.numAttempts());
                return;
            } catch (Exception | AssertionError e) {
                // This includes AssertionErrors because of tests where a flaky operation takes place, and then
                // assertions are made on the state of the world assuming that said flaky operation was successful.
                // TODO (jkong): Make whether AssertionError is permitted configurable.
                logFailureAndThrowIfNeeded(retryAnnotation, description, attempt, e);
            } catch (Throwable t) {
                // This covers other Errors, where it generally doesn't make sense to retry.
                throw Throwables.propagate(t);
            }
        }
    }

    private static void logFailureAndThrowIfNeeded(
            ShouldRetry retryAnnotation,
            Description description,
            int attempt,
            Throwable e) {
        log.info("Test {}.{} failed on attempt {} of {}.",
                description.getClassName(),
                description.getMethodName(),
                attempt,
                retryAnnotation.numAttempts(),
                e);
        if (attempt == retryAnnotation.numAttempts()) {
            throw Throwables.propagate(e);
        }
    }
}
