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
package com.palantir.flake.fail;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;

public final class ExpectedFailureExtension implements AfterTestExecutionCallback, TestExecutionExceptionHandler {
    private static final SafeLogger log = SafeLoggerFactory.get(ExpectedFailureExtension.class);

    private boolean exceptionInCurrentIteration = false;

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable) throws Throwable {
        log.info(
                "Test {} failed, which is in line with expectations.",
                SafeArg.of("testName", getTestName(extensionContext)),
                throwable);

        exceptionInCurrentIteration = true;
        if (throwable instanceof TestAbortedException) {
            throw throwable;
        }
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) {
        if (!exceptionInCurrentIteration) {
            throw new AssertionError(
                    String.format("%s was expected to fail, but passed!", getTestName(extensionContext)));
        }
        exceptionInCurrentIteration = false;
    }

    private static String getTestName(ExtensionContext extensionContext) {
        return String.format(
                "%s.%s",
                extensionContext.getRequiredTestClass().getName(),
                extensionContext.getRequiredTestMethod().getName());
    }
}
