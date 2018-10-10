/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cassandra;

import java.util.function.LongUnaryOperator;

/**
 * This interface produces Cassandra timestamps for mutations, where it makes sense for these to be overridden.
 * The values returned by this provider must observe the following semantics.
 *
 *   - Sweep sentinels: These must be strictly greater than the Cassandra timestamps of any previous deletions or range
 *     tombstones covering the sweep sentinel. (We may want to write sweep sentinels to a table where deletions or range
 *     tombstones already exist, in the case of tables shifting between THOROUGH to CONSERVATIVE sweep.)
 *   - Deletions: These must be strictly greater than the Cassandra timestamp at which the original value being deleted
 *     was written.
 *   - Range tombstones: These must be strictly greater than the Cassandra timestamps of all values covered by the
 *     range tombstone (including sweep sentinels, if applicable.)
 *
 * In addition to the above semantics, these values should increase over time and should largely be in line with
 * AtlasDB's notion of logical time, to ensure smooth compaction of tables.
 */
public interface CassandraMutationTimestampProvider {
    /**
     * Returns the Cassandra timestamp at which sweep sentinels should be written.
     *
     * This is called once per API call to CassandraKeyValueServiceImpl.addGarbageCollectionSentinelValues().
     */
    long getSweepSentinelWriteTimestamp();

    /**
     * Returns the Cassandra timestamp at which whole row deletes should be performed.
     */
    long getRemoveTimestamp();

    /**
     * Returns an operator, which determines the Cassandra timestamp to write a delete at, given the Atlas timestamp
     * at which the deletion occurred.
     * The operator is intended to be use for a single batch deletion, and must not be re-used outside of the
     * context of a single batch delete.
     *
     * The operator is obtained once per API call to CassandraKeyValueServiceImpl.delete().
     * The operator is invoked once per cell involved in such an API call, and thus must be cheap to invoke.
     */
    LongUnaryOperator getDeletionTimestampOperatorForBatchDelete();

    /**
     * Cassandra timestamp at which a range tombstone should be written.
     * @param maximumAtlasTimestampExclusive Maximum timestamp, exclusive, of the range tombstone.
     *
     * This is called once per API call to CassandraKeyValueServiceImpl.deleteAllTimestamps().
     */
    long getRangeTombstoneTimestamp(long maximumAtlasTimestampExclusive);
}
