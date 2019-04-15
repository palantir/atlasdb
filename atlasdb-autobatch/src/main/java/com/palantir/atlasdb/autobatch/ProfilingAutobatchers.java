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

import java.util.List;
import java.util.function.Consumer;

public final class ProfilingAutobatchers {

    private ProfilingAutobatchers() {
        // factory
    }

    public static <T, R> DisruptorAutobatcher<T, R> create(
            String safeIdentifier,
            Consumer<List<BatchElement<T, R>>> batchFunction) {
        BatchSizeLogger batchSizeLogger = BatchSizeLogger.create(safeIdentifier);
        return DisruptorAutobatcher.create(elements -> {
            batchFunction.accept(elements);

            // Shouldn't affect clients, because futures have already been completed
            batchSizeLogger.markBatchProcessed(elements.size());
        });
    }
}
