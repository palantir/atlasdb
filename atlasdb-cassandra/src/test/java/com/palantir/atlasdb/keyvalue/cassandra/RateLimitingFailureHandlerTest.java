/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import org.junit.Test;

public class RateLimitingFailureHandlerTest {
    @Test
    public void callsDelegateMultipleTimesIfIntervalHasElapsed() {
        Runnable delegate = mock(Runnable.class);
        RateLimitingFailureHandler failureHandler = RateLimitingFailureHandler.create(delegate, Duration.ofNanos(1));
        failureHandler.run();
        Uninterruptibles.sleepUninterruptibly(Duration.ofNanos(10));
        failureHandler.run();
        verify(delegate, times(2)).run();
    }

    @Test
    public void callsDelegateOnlyOnceIfIntervalHasNotElapsed() {
        Runnable delegate = mock(Runnable.class);
        RateLimitingFailureHandler failureHandler = RateLimitingFailureHandler.create(delegate, Duration.ofDays(1));
        failureHandler.run();
        failureHandler.run();
        verify(delegate).run();
    }
}
