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
package com.palantir.cassandra.multinode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import org.junit.Test;

public class OneNodeDownDeleteTest extends AbstractDegradedClusterTest {

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void deletingThrows() {
        assertThrowsInsufficientConsistencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().delete(TEST_TABLE, ImmutableMultimap.of(CELL_1_1, TIMESTAMP)));
    }

    @Test
    public void deleteAllTimestampsThrows() {
        assertThrowsInsufficientConsistencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().deleteAllTimestamps(
                        TEST_TABLE,
                        ImmutableMap.of(CELL_1_1, new TimestampRangeDelete.Builder()
                                .timestamp(TIMESTAMP)
                                .endInclusive(false)
                                .deleteSentinels(false)
                                .build())));
    }
}
