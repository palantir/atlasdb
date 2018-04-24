/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.batch;

import java.io.Closeable;
import java.util.function.Function;

public interface BatchingTaskRunner extends Closeable {

    interface BatchingStrategy<InT> {
        Iterable<? extends InT> partitionIntoBatches(InT collection, int batchSizeHint);
    }

    interface ResultAccumulatorStrategy<OutT> {
        OutT createEmptyResult();
        void accumulateResult(OutT result, OutT toAdd);
    }

    <InT, OutT> OutT runTask(InT input,
                             BatchingStrategy<InT> batchingStrategy,
                             ResultAccumulatorStrategy<OutT> resultAccumulatingStrategy,
                             Function<InT, OutT> task);

    @Override
    void close();
}
