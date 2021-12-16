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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import org.junit.Test;

public class CassandraKeyValueServicesTest {
    private static final byte[] DATA = PtBytes.toBytes("data");

    private static final Cell CELL = Cell.create(DATA, DATA);

    @Test
    public void createColumnWithGivenValueCreatesItWithAssociatedCassandraTimestamp() {
        assertThat(CassandraKeyValueServices.createColumn(CELL, Value.create(DATA, 1000))
                        .getTimestamp())
                .isEqualTo(1000);
        assertThat(CassandraKeyValueServices.createColumn(CELL, Value.create(DATA, 5000))
                        .getTimestamp())
                .isEqualTo(5000);
    }

    @Test
    public void createColumnForDeleteCreatesItWithSpecifiedCassandraTimestamp() {
        assertThat(CassandraKeyValueServices.createColumnForDelete(
                                CELL, Value.create(PtBytes.EMPTY_BYTE_ARRAY, 1000), 2000)
                        .getTimestamp())
                .isEqualTo(2000);
    }
}
