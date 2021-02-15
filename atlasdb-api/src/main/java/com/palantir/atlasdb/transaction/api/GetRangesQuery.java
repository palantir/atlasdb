/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.BatchingVisitable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import org.immutables.value.Value;

@Value.Immutable
public interface GetRangesQuery<RESPONSE_TYPE> {
    TableReference tableRef();

    Iterable<RangeRequest> rangeRequests();

    /**
     * Parallelism to run this getRanges query with. If not specified, a default value is selected.
     */
    Optional<Integer> concurrencyLevel();

    /**
     * An operator invoked on each range request, to possibly improve the performance of range requests based on schema
     * knowledge. The output of this operator is only used for internal queries: the user-provided visitable processor
     * will receive the original range requests from {@link #rangeRequests()}.
     */
    @Value.Default
    default UnaryOperator<RangeRequest> rangeRequestOptimizer() {
        return x -> x;
    }

    BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, RESPONSE_TYPE> visitableProcessor();
}
