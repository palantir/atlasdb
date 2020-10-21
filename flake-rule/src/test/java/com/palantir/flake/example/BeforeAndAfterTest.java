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
package com.palantir.flake.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class BeforeAndAfterTest {
    private static final AtomicInteger attemptCount = new AtomicInteger();
    private static final int SEVENTY_SEVEN = 77;

    private static Runnable beforeRunnable = mock(Runnable.class);
    private static Runnable afterRunnable = mock(Runnable.class);
    private static Runnable beforeClassRunnable = mock(Runnable.class);
    private static Runnable afterClassRunnable = mock(Runnable.class);

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    @BeforeClass
    public static void setUpClass() {
        beforeClassRunnable.run();
    }

    @Before
    public void setUp() {
        beforeRunnable.run();
    }

    @After
    public void tearDown() {
        afterRunnable.run();
    }

    @AfterClass
    public static void tearDownClass() {
        afterClassRunnable.run();
    }

    @Test
    @ShouldRetry(numAttempts = SEVENTY_SEVEN)
    public void runsSetUpBeforeEachIteration() {
        int attemptNumber = attemptCount.incrementAndGet();

        verify(beforeClassRunnable, times(1)).run();
        verify(beforeRunnable, times(attemptNumber)).run();
        verify(afterRunnable, times(attemptNumber - 1)).run();
        verify(afterClassRunnable, never()).run();

        assertThat(attemptNumber).isEqualTo(SEVENTY_SEVEN);
    }
}
