/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public final class NoOpWorkerPoolTest {
    private final WorkerPool workerPool = NoOpWorkerPool.INSTANCE;

    @Test
    public void noOpWorkerPoolCloseReturnsEmpty() {
        assertThat(workerPool.close()).isEmpty();
    }

    @Test
    public void noOpWorkerPoolTryRunReturnsFalse() {
        assertThat(workerPool.tryRun(() -> 1, $ -> {})).isFalse();
    }

    @Test
    public void noOpWorkerPoolDoesNotRunTask() {
        AtomicBoolean taskWasRun = new AtomicBoolean(false);
        workerPool.tryRun(() -> 1, $ -> taskWasRun.set(true));
        assertThat(taskWasRun).isFalse();
    }

    @Test
    public void noOpWorkPoolDoesNotConsumeSupplier() {
        AtomicBoolean supplierWasConsumed = new AtomicBoolean(false);
        workerPool.tryRun(
                () -> {
                    supplierWasConsumed.set(true);
                    return 1;
                },
                $ -> {});
        assertThat(supplierWasConsumed).isFalse();
    }
}
