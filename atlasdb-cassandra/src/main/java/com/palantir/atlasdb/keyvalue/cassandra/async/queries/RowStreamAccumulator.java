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

package com.palantir.atlasdb.keyvalue.cassandra.async.queries;

import com.datastax.driver.core.Row;
import java.util.stream.Stream;

/**
 * {@code RowStreamAccumulator} defines an interface which should be implemented to process {@code Stream} of
 * {@code Row}s retrieved from Cassandra.
 *
 * @param <R> type of the result of accumulating all rows
 */
public interface RowStreamAccumulator<R> {

    /**
     * Processes each row and updates the internal state of the instance. After each invocation of this method calling
     * {@code result} should return the accumulated result of rows processed up to that moment. Implementations should
     * not block during processing of the passed stream as that would prevent the thread from doing other work.
     * If invoked concurrently with either {@code accumulateRowStream} or {@code result} the behaviour is not defined.
     * @param rowStream of available rows without blocking
     */
    void accumulateRowStream(Stream<Row> rowStream);

    /**
     * Should be called after all intended streams are processed. Will return the current state of the accumulator
     * without knowing if it is the end result. If invoked concurrently with either{@code accumulateRowStream}
     * or {@code result} the behaviour is not defined.
     *
     * @return accumulated result
     */
    R result();
}
