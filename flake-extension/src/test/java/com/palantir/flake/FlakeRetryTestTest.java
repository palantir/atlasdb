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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.flake.fail.ExpectedFailure;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class FlakeRetryTestTest {
    private static final Map<String, AtomicLong> counters = new HashMap<>();

    @FlakeRetryTest(maxNumberOfRetriesUntilSuccess = 2)
    public void acceptsIfWePassOnTheFirstAttemptOfTwo(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 1);
    }

    @FlakeRetryTest(maxNumberOfRetriesUntilSuccess = 2)
    public void acceptsIfWePassOnTheSecondAttemptOfTwo(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 2);
    }

    @ExpectedFailure
    @FlakeRetryTest(maxNumberOfRetriesUntilSuccess = 2)
    public void doesNotRetryMoreThanSpecifiedNumberOfTimes(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 3);
    }

    @FlakeRetryTest(maxNumberOfRetriesUntilSuccess = 100)
    public void canConfigureNumberOfAttempts(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 100);
    }

    @ExpectedFailure
    @Test
    public void doesNotRetryIfMethodIsNotAnnotated(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 2);
    }

    @ExpectedFailure
    @FlakeRetryTest(retryableExceptions = {RuntimeException.class})
    public void doesNotRetryIfNotRetryable(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 2);
    }

    @FlakeRetryTest(retryableExceptions = {Throwable.class})
    public void retriesIfSuperOfRetriable(TestInfo testInfo) {
        runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 2);
    }

    @FlakeRetryTest(retryableExceptions = {IllegalStateException.class})
    public void retriesIfCauseIsRetryable(TestInfo testInfo) {
        try {
            runTestFailingUntilSpecifiedAttempt(getTestName(testInfo), 2);
        } catch (AssertionError e) {
            throw new RuntimeException(new IllegalStateException());
        }
    }

    private void runTestFailingUntilSpecifiedAttempt(String name, long expected) {
        AtomicLong counter = counters.getOrDefault(name, new AtomicLong());
        long value = counter.incrementAndGet();
        counters.put(name, counter);
        assertThat(value).isEqualTo(expected);
    }

    private static String getTestName(TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName();
    }
}
