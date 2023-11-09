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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * An annotation signalling that a given method is a test and should be retried in the event of a test failure.
 * <p>
 * Note that this annotation is already annotated with {@link ExtendWith}({@link FlakeRetryExtension}), so test methods
 * shouldn't be annotated with {@link ExtendWith}({@link FlakeRetryExtension}).
 * <p>
 * Note that this annotation is already annotated with {@link TestTemplate}, so test methods shouldn't be annotated with
 * {@link TestTemplate} or {@link Test}.
 * <p>
 * Note that this annotation does not work with parameterized tests.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
@ExtendWith(FlakeRetryExtension.class)
public @interface FlakeRetryTest {
    /**
     * The number of attempts to retry a test that fails, before declaring the test as failed.
     * This is useful for avoiding build failures whilst flaky tests still live in the codebase.
     * Values provided should be strictly positive otherwise the test will fail.
     */
    int maxNumberOfRetriesUntilSuccess() default 5;

    /**
     * Array specifying which types of Throwables should be retried. The default value throws a very wide net, and
     * should ideally be replaced by a more specific list.
     */
    Class<? extends Throwable>[] retryableExceptions() default {AssertionError.class, Exception.class};
}
