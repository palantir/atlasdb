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

package com.palantir.atlasdb.autobatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class BatchSizeLoggerTest {
    private static final String SAFE_IDENTIFIER = "identifier";

    @Test
    public void computesAverage() {
        BatchSizeLogger batchSizeLogger = new BatchSizeLogger(() -> false, SAFE_IDENTIFIER);
        batchSizeLogger.markBatchProcessed(5);
        batchSizeLogger.markBatchProcessed(10);
        assertThat(batchSizeLogger.getCurrentAverage()).isEqualTo(7.5);
    }

    @Test
    public void resetsWhenFlushing() {
        BatchSizeLogger batchSizeLogger = new BatchSizeLogger(() -> true, SAFE_IDENTIFIER);
        batchSizeLogger.markBatchProcessed(5);
        assertThat(batchSizeLogger.getCurrentAverage()).isNaN();
    }

    @Test
    public void recoversFromReset() {
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        BatchSizeLogger batchSizeLogger = new BatchSizeLogger(atomicBoolean::get, SAFE_IDENTIFIER);
        batchSizeLogger.markBatchProcessed(8);
        assertThat(batchSizeLogger.getCurrentAverage()).isEqualTo(8.0);

        atomicBoolean.set(true);
        batchSizeLogger.markBatchProcessed(6);

        atomicBoolean.set(false);
        batchSizeLogger.markBatchProcessed(4);
        assertThat(batchSizeLogger.getCurrentAverage()).isEqualTo(4.0);
    }
}
