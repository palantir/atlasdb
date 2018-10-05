/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.common.exception.AtlasDbDependencyException;

public class OneNodeDownDeleteTest extends AbstractDegradedClusterTest {

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void deletingThrows() {
        assertThatThrownBy(() -> getTestKvs().delete(TEST_TABLE, ImmutableMultimap.of(CELL_1_1, TIMESTAMP)))
                .isInstanceOf(AtlasDbDependencyException.class);
    }

    @Test
    public void deleteAllTimestampsThrows() {
        assertThatThrownBy(() -> getTestKvs().deleteAllTimestamps(TEST_TABLE,
                ImmutableMap.of(CELL_1_1, TIMESTAMP), false))
                .isInstanceOf(AtlasDbDependencyException.class);
    }
}
