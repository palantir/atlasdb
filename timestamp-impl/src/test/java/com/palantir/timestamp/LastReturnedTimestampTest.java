/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.timestamp;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

public class LastReturnedTimestampTest {
    private static final long INITIAL_VALUE = 100;

    LastReturnedTimestamp timestamp = new LastReturnedTimestamp(INITIAL_VALUE);
    private ExecutorService executor = Executors.newFixedThreadPool(4);

    @Test
    public void shouldIncreaseTheValueToAHigherNumber() {
        timestamp.increaseToAtLeast(INITIAL_VALUE + 10);

        assertThat(timestamp.get(), is(INITIAL_VALUE + 10));
    }

    @Test
    public void shouldNotIncreaseTheValueToALowerNumber() {
        timestamp.increaseToAtLeast(INITIAL_VALUE - 10);

        assertThat(timestamp.get(), is(INITIAL_VALUE));
    }

    @Test
    public void handleConcurrentlyIncreasingTheValue() throws InterruptedException {
        for(int i = 0; i <= 100; i++) {
            final int value = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    timestamp.increaseToAtLeast(INITIAL_VALUE + value);
                }
            });
        }

        waitForExecutorToFinish();

        assertThat(timestamp.get(), is(INITIAL_VALUE + 100));
    }

    private void waitForExecutorToFinish() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, SECONDS);
    }
}
