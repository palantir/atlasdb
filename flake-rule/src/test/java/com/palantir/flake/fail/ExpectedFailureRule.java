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
package com.palantir.flake.fail;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class ExpectedFailureRule implements TestRule {
    private static final SafeLogger log = SafeLoggerFactory.get(ExpectedFailureRule.class);

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                if (isExpectedToPass(description)) {
                    base.evaluate();
                    return;
                }
                evaluateBaseExpectingFailure();
            }

            @SuppressWarnings("CatchBlockLogException")
            private void evaluateBaseExpectingFailure() {
                try {
                    base.evaluate();
                    throw new AssertionError(
                            String.format("%s was expected to fail, but passed!", getTestName(description)));
                } catch (Throwable t) {
                    // PASS - a failure was expected
                    log.info(
                            "Test {} failed, which is in line with expectations.",
                            SafeArg.of("testName", getTestName(description)));
                }
            }
        };
    }

    private static String getTestName(Description description) {
        return String.format("%s.%s", description.getClassName(), description.getMethodName());
    }

    private static boolean isExpectedToPass(Description description) {
        return description.getAnnotation(ExpectedFailure.class) == null;
    }
}
