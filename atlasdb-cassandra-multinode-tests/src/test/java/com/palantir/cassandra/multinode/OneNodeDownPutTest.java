/**
 * Copyright 2016 Palantir Technologies
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

import static org.junit.Assert.assertEquals;

import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_1_1;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_1_2;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_2_1;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_2_2;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_3_1;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_3_2;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.DEFAULT_VALUE;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.TEST_TABLE;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.db;

import java.util.HashMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;

public class OneNodeDownPutTest {

    private final byte[] NEW_VALUE = "new_value".getBytes();
    private final long NEW_TIMESTAMP = 7L;

    @Rule
    public ExpectedException expect_exception = ExpectedException.none();

    @Test
    public void canPut() {
        db.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, NEW_VALUE), NEW_TIMESTAMP);
        OneNodeDownGetTest.verifyTimestampAndValue(CELL_1_1, NEW_TIMESTAMP, NEW_VALUE);
    }

    @Test
    public void canPutWithTimestamps() {
        db.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(CELL_1_2, Value.create(NEW_VALUE, NEW_TIMESTAMP)));
        OneNodeDownGetTest.verifyTimestampAndValue(CELL_1_2, NEW_TIMESTAMP, NEW_VALUE);
    }

    @Test
    public void canMultiPut() {
        HashMap<Cell, byte[]> entries = new HashMap();
        entries.put(CELL_2_1, NEW_VALUE);
        entries.put(CELL_2_2, NEW_VALUE);

        db.multiPut(ImmutableMap.of(TEST_TABLE, entries), NEW_TIMESTAMP);

        OneNodeDownGetTest.verifyTimestampAndValue(CELL_2_1, NEW_TIMESTAMP, NEW_VALUE);
        OneNodeDownGetTest.verifyTimestampAndValue(CELL_2_2, NEW_TIMESTAMP, NEW_VALUE);
    }

    @Test
    public void canPutUnlessExists() {
        expect_exception.expect(KeyAlreadyExistsException.class);
        db.putUnlessExists(TEST_TABLE, ImmutableMap.of(CELL_3_2, DEFAULT_VALUE));
        OneNodeDownGetTest.verifyTimestampAndValue(CELL_3_2, AtlasDbConstants.TRANSACTION_TS, DEFAULT_VALUE);
    }

    @Test
    public void putUnlessExistsThrowsOnExists() {
        expect_exception.expect(KeyAlreadyExistsException.class);
        db.putUnlessExists(TEST_TABLE, ImmutableMap.of(CELL_1_1, DEFAULT_VALUE));
    }

    @Test
    public void canAddGarbageCollectionSentinelValues() {
        db.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(CELL_3_1));
        assertEquals(new Long(Value.INVALID_VALUE_TIMESTAMP),
                db.getLatestTimestamps(TEST_TABLE, ImmutableMap.of(CELL_3_1, Long.MAX_VALUE)).get(CELL_3_1));
    }
}
