/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import java.util.function.LongUnaryOperator;

/**
 * This interface produces Cassandra timestamps for mutations, where it makes sense for these to be overridden.
 * Generally, to ensure that Cassandra is able to compact tombstones smoothly, these values should increase over
 * time and should largely be in line with AtlasDB's notion of logical time.
 */
public interface CassandraMutationTimestampProvider {
    /**
     * Returns the Cassandra timestamp at which sweep sentinels should be written.
     */
    long getSweepSentinelWriteTimestamp();

    /**
     * Returns an operator, which determines the Cassandra timestamp to write a delete at, given the Atlas timestamp
     * at which the deletion occurred.
     * The operator is intended to be use for a single batch deletion, and must not be re-used outside of the
     * context of a single batch delete.
     *
     * This operator may be invoked once per cell involved in a batch delete, and thus must be cheap to invoke.
     */
    LongUnaryOperator getDeletionTimestampOperatorForBatchDelete();

    /**
     * Cassandra timestamp at which a range tombstone should be written.
     * @param maximumAtlasTimestampExclusive Maximum timestamp, exclusive, of the range tombstone.
     */
    long getRangeTombstoneTimestamp(long maximumAtlasTimestampExclusive);
}
