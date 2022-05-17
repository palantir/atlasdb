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

package com.palantir.atlasdb.keyvalue.api;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.metrics.Timed;
import com.palantir.common.annotation.Idempotent;
import java.util.Map;

public interface AsyncKeyValueService extends AutoCloseable {
    /**
     * Asynchronously gets values from the key-value store when the store allows it. In other cases it just wraps the
     * result in an immediate future.
     *
     * @param tableRef        the name of the table to retrieve values from.
     * @param timestampByCell specifies, for each row, the maximum timestamp (exclusive) at which to retrieve that
     *                        row's value.
     *
     * @return listenable future containing map of retrieved values. Values which do not exist (either because they were
     * deleted or never created in the first place) are simply not returned.
     */
    @Idempotent
    @Timed
    ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell);

    @Override
    void close();

    /**
     * Denotes whether the AsyncKeyValueService is currently capable of handling requests.
     * Reasons for an AsyncKeyValueService to be invalid include e.g being closed, in the process of being refreshed, or
     * incorrectly configured.
     *
     * Use this value to shortcircuit async requests. However, it is not guaranteed that a {@link #getAsync} call will
     * not fail even after isValid has returned true - the AsyncKeyValueService may transition to invalid between the
     * `isValid` call and a `getAsync` call.
     *
     * @return true iff the AsyncKeyValueService is currently capable of handling requests at the instant this method
     * was called, otherwise false.
     */
    default boolean isValid() {
        return true;
    }
}
