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

import com.google.common.collect.Maps;
import com.palantir.flake.fail.ExpectedFailure;
import com.palantir.flake.fail.ExpectedFailureRule;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

public class FlakeRetryingRuleTest {
    private static final Map<String, AtomicLong> counters = Maps.newHashMap();

    private final FlakeRetryingRule retryingRule = new FlakeRetryingRule();
    private final ExpectedFailureRule expectedFailureRule = new ExpectedFailureRule();

    // The ordering here is essential. We want to try the inner test multiple times and invert the output in some
    // cases (because we're expecting to fail out), not invert the output on each attempt.
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(expectedFailureRule)
            .around(retryingRule);

    // The ordering of this rule with respect to the rule chain is not essential, as it is merely used to
    // ensure consistency of indexing into the counter-map for the runTestFailingUntilSpecifiedAttempt methods.
    @Rule
    public final TestName testName = new TestName();

    @Test
    @ShouldRetry(numAttempts = 2)
    public void acceptsIfWePassOnTheFirstAttemptOfTwo() {
        runTestFailingUntilSpecifiedAttempt(1);
    }

    @Test
    @ShouldRetry(numAttempts = 2)
    public void acceptsIfWePassOnTheSecondAttemptOfTwo() {
        runTestFailingUntilSpecifiedAttempt(2);
    }

    @Test
    @ShouldRetry(numAttempts = 2)
    @ExpectedFailure
    public void doesNotRetryMoreThanSpecifiedNumberOfTimes() {
        runTestFailingUntilSpecifiedAttempt(3);
    }

    @Test
    @ShouldRetry(numAttempts = 100)
    public void canConfigureNumberOfAttempts() {
        runTestFailingUntilSpecifiedAttempt(100);
    }

    @Test
    @ExpectedFailure
    public void doesNotRetryIfMethodIsNotAnnotated() {
        runTestFailingUntilSpecifiedAttempt(2);
    }

    @Test
    @ShouldRetry(numAttempts = -5)
    @ExpectedFailure // This should trigger, because the number of attempts should be positive.
    public void cannotConfigureNegativeNumberOfAttempts() {
        // pass
    }

    @Test
    @ShouldRetry(retryableExceptions = {RuntimeException.class})
    @ExpectedFailure
    public void doesNotRetryIfNotRetryable() {
        runTestFailingUntilSpecifiedAttempt(2);
    }

    @Test
    @ShouldRetry(retryableExceptions = {Throwable.class})
    public void retriesIfSuperOfRetriable() {
        runTestFailingUntilSpecifiedAttempt(2);
    }

    @Test
    @ShouldRetry(retryableExceptions = {IllegalStateException.class})
    public void retriesIfCauseIsRetryable() {
        try {
            runTestFailingUntilSpecifiedAttempt(2);
        } catch (AssertionError e) {
            throw new RuntimeException(new IllegalStateException());
        }
    }

    private void runTestFailingUntilSpecifiedAttempt(long expected) {
        AtomicLong counter = counters.getOrDefault(testName.getMethodName(), new AtomicLong());
        long value = counter.incrementAndGet();
        counters.put(testName.getMethodName(), counter);
        assertThat(value).isEqualTo(expected);
    }
}
