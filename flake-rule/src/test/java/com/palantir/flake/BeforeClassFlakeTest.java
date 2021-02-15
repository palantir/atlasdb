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

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@ShouldRetry(numAttempts = 2)
public class BeforeClassFlakeTest {
    private static AtomicBoolean succeed = new AtomicBoolean(false);

    @ClassRule
    public static final FlakeRetryingRule flakeRetryingRule = new FlakeRetryingRule();

    @BeforeClass
    public static void secondTimePassingSetup() {
        boolean value = succeed.getAndSet(true);
        if (!value) {
            throw new IllegalStateException("expected value to be true");
        }
    }

    @Test
    public void passingTest() {
        assertThat(1).isGreaterThan(0);
    }
}
