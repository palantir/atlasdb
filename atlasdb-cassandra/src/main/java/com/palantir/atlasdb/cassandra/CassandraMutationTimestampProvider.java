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

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * This interface produces Cassandra timestamps for mutations, where it makes sense for these to be overridden.
 * Generally, to ensure that Cassandra is able to compact tombstones smoothly, these values should increase over
 * time and should largely be in line with AtlasDB's notion of logical time.
 *
 * Calling these methods must be cheap, at least on an amortised basis; they may be called multiple times
 * in rapid succession (as frequently as once per cell) when
 * {@link com.palantir.atlasdb.keyvalue.api.KeyValueService#delete(TableReference, Multimap)} or other deletion
 * methods are called.
 */
public interface CassandraMutationTimestampProvider {
    /**
     * Cassandra timestamp at which sweep sentinels should be written.
     */
    long getSweepSentinelWriteTimestamp();

    /**
     * Cassandra timestamp at which KVS-level deletes should be performed.
     * @param atlasDeletionTimestamp Atlas timestamp of the relevant delete.
     */
    long getDeletionTimestamp(long atlasDeletionTimestamp);

    /**
     * Cassandra timestamp at which a range tombstone should be written.
     * @param maximumAtlasTimestampExclusive Maximum timestamp, exclusive, of the range tombstone.
     */
    long getRangeTombstoneTimestamp(long maximumAtlasTimestampExclusive);
}
