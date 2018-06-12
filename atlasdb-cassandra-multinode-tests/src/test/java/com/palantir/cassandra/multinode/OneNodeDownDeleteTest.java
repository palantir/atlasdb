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
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;

public class OneNodeDownDeleteTest {
    private static final String REQUIRES_ALL_CASSANDRA_NODES = "requires ALL Cassandra nodes to be up and available.";

    @Test
    public void deletingThrows() {
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.delete(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMultimap.of(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_TIMESTAMP)))
                .isExactlyInstanceOf(InsufficientConsistencyException.class)
                .hasMessageContaining(REQUIRES_ALL_CASSANDRA_NODES);
    }

    @Test
    public void deleteAllTimestampsThrows() {
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.deleteAllTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_TIMESTAMP), false))
                .isExactlyInstanceOf(InsufficientConsistencyException.class)
                .hasMessageContaining(REQUIRES_ALL_CASSANDRA_NODES);
    }
}
