/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.opentest4j.TestAbortedException;

/**
 * An {@link Extension} that retries methods annotated with the {@link FlakeRetryTest} annotation. This class shouldn't
 * be extended by test methods. It should be used via the {@link FlakeRetryTest} annotation.
 * <p>
 * Like other JUnit Extensions, this extension respects lifecycle of test methods. Thus, methods annotated
 * {@link BeforeEach} will run before *each* attempt at running a test, and methods annotated
 * with {@link AfterEach} will similarly run after each attempt. However, since retrying takes place at the method level,
 * methods annotated with {@link BeforeAll} and {@link AfterAll} will NOT be re-run.
 * <p>
 * To illustrate this, consider a test class with one flaky method that has been annotated with {@link FlakeRetryTest}.
 * The order in which methods are run is as follows:
 * <pre>
 *     BeforeAll runs
 *     BeforeEach runs
 *     Test fails on attempt 1
 *     AfterEach runs
 *     BeforeEach runs
 *     Test succeeds on attempt 2
 *     AfterEach runs
 *     AfterAll runs
 * </pre>
 * This extension is intended to catch flaky cases in the test method itself. It will not catch flaky implementations of
 * {@link BeforeAll}, {@link BeforeEach}, {@link AfterAll}, {@link AfterEach}.
 * <p>
 * Note: Please be very careful about ordering when chaining this with other JUnit {@link Extension}s.
 */
final class FlakeRetryExtension
        implements TestTemplateInvocationContextProvider, AfterTestExecutionCallback, TestExecutionExceptionHandler {

    private static final SafeLogger log = SafeLoggerFactory.get(FlakeRetryExtension.class);

    private int maxNumberOfRetriesUntilSuccess;
    private List<Class<? extends Throwable>> retryableExceptions;

    private int numberOfRetriesSoFar = 1; // Includes the initial attempt
    private boolean hasSucceeded = false;
    private boolean hasCompletelyFailed = false;

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return isAnnotated(extensionContext.getTestMethod(), FlakeRetryTest.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext extensionContext) {
        FlakeRetryTest testWithFlakeRetry = findAnnotation(extensionContext.getTestMethod(), FlakeRetryTest.class)
                .orElseThrow(() -> new IllegalStateException(
                        "Expected to find @TestWithFlakeRetry annotation on the test method, but couldn't find."
                                + " This is likely to be a problem with " + FlakeRetryExtension.class.getName()
                                + " implementation."));

        retryableExceptions = List.of(testWithFlakeRetry.retryableExceptions());
        maxNumberOfRetriesUntilSuccess = testWithFlakeRetry.maxNumberOfRetriesUntilSuccess();
        Preconditions.checkArgument(
                maxNumberOfRetriesUntilSuccess > 0,
                "Number of retries until success should be positive.",
                SafeArg.of("maxNumberOfRetriesUntilSuccess", maxNumberOfRetriesUntilSuccess));

        return stream(spliteratorUnknownSize(new TestTemplateInvocationContextIterator(), Spliterator.NONNULL), false);
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) {
        if (extensionContext.getExecutionException().isEmpty()) {
            log.info(
                    "Test {}.{} succeeded on attempt {} of {}.",
                    SafeArg.of(
                            "testClass", extensionContext.getRequiredTestClass().getName()),
                    SafeArg.of(
                            "testMethod",
                            extensionContext.getRequiredTestMethod().getName()),
                    SafeArg.of("numberOfRetries", numberOfRetriesSoFar),
                    SafeArg.of("maxAttempts", maxNumberOfRetriesUntilSuccess));
            hasSucceeded = true;
        }
        numberOfRetriesSoFar++;
    }

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        if (!isExceptionInRetryableExceptions(throwable)) {
            hasCompletelyFailed = true;
            throw throwable;
        }

        log.info(
                "Test {}.{} failed on attempt {} of {}.",
                SafeArg.of("testClass", extensionContext.getRequiredTestClass().getName()),
                SafeArg.of(
                        "testMethod", extensionContext.getRequiredTestMethod().getName()),
                SafeArg.of("numberOfRetries", numberOfRetriesSoFar),
                SafeArg.of("maxAttempts", maxNumberOfRetriesUntilSuccess),
                throwable);
        if (maxNumberOfRetriesUntilSuccess == numberOfRetriesSoFar) {
            hasCompletelyFailed = true;
            throw throwable;
        }
        throw new TestAbortedException("Retry the test because it might be flaky.", throwable);
    }

    private boolean isExceptionInRetryableExceptions(Throwable throwable) {
        return retryableExceptions.stream().anyMatch(ex -> causeHasType(throwable, ex));
    }

    private static boolean causeHasType(Throwable cause, Class<? extends Throwable> type) {
        return cause != null && (type.isInstance(cause) || causeHasType(cause.getCause(), type));
    }

    private final class TestTemplateInvocationContextIterator implements Iterator<TestTemplateInvocationContext> {
        @Override
        public boolean hasNext() {
            return !hasCompletelyFailed && !hasSucceeded && numberOfRetriesSoFar <= maxNumberOfRetriesUntilSuccess;
        }

        @Override
        public TestTemplateInvocationContext next() {
            return new TestTemplateInvocationContext() {};
        }
    }
}
