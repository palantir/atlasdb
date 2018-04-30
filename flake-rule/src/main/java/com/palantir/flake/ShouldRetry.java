/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation signalling that a given test class or test method should be retried in the event of a test failure.
 * If specified on a test class, all test methods in the class will be considered retriable. In the event that both a
 * test class and a test method have this annotation, the method-level annotation will take precedence.
 *
 * Note that this annotation will only actually cause a test to be retried if it is run with the
 * {@link FlakeRetryingRule} present as a test rule in the relevant class.
 *
 * Please consult the documentation for {@link FlakeRetryingRule} for details concerning interactions between
 * test retrying and other JUnit constructs.
 */
@Retention(RetentionPolicy.RUNTIME) // The FlakeRetryingRule makes retry decisions based on this annotation at runtime.
public @interface ShouldRetry {
    /**
     * The number of attempts to retry a test that fails, before declaring the test as failed.
     * This is useful for avoiding build failures whilst flaky tests still live in the codebase.
     *
     * Values provided should be strictly positive; behaviour when this value is zero or negative is undefined.
     */
    int numAttempts() default 5;
}
