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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

@ShouldRetry(numAttempts = 2)
public class FlakeRetryingRuleClassAnnotationTest {
    private static final Map<String, AtomicLong> counters = Maps.newHashMap();

    @Rule
    public final FlakeRetryingRule retryingRule = new FlakeRetryingRule();

    @Rule
    public final TestName testName = new TestName();

    @Test
    public void retriesBasedOnClassLevelAnnotation() {
        runTestFailingUntilSpecifiedAttempt(2);
    }

    @Test
    @ShouldRetry(numAttempts = 10)
    public void methodLevelAnnotationTakesPrecedence() {
        runTestFailingUntilSpecifiedAttempt(10);
    }

    private void runTestFailingUntilSpecifiedAttempt(long expected) {
        AtomicLong counter = counters.getOrDefault(testName.getMethodName(), new AtomicLong());
        long value = counter.incrementAndGet();
        counters.put(testName.getMethodName(), counter);
        assertThat(value).isEqualTo(expected);
    }
}
