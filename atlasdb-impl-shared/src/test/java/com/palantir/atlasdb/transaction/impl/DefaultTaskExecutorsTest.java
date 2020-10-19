/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class DefaultTaskExecutorsTest {
    @Test
    public void deleteExecutorHasBoundedTaskQueue() {
        ExecutorService service = DefaultTaskExecutors.createDefaultDeleteExecutor();
        for (int i = 0; i < DefaultTaskExecutors.DEFAULT_QUEUE_CAPACITY + 1; i++) {
            service.submit(() -> {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.DAYS);
                return 1;
            });
        }
        assertThatThrownBy(() -> service.submit(() -> 0)).isInstanceOf(RejectedExecutionException.class)
                .hasMessageContaining("rejected from");
    }
}
