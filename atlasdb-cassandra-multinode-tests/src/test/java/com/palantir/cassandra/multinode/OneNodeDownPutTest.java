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

import java.util.Map;

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

    private final byte[] newValue = "new_value".getBytes();
    private final long newTimestamp = 7L;

    @Rule
    public ExpectedException expectException = ExpectedException.none();

    @Test
    public void canPut() {
        OneNodeDownTestSuite.db.put(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_1_1, newValue), newTimestamp);
        OneNodeDownGetTest.verifyTimestampAndValue(OneNodeDownTestSuite.CELL_1_1, newTimestamp, newValue);
    }

    @Test
    public void canPutWithTimestamps() {
        OneNodeDownTestSuite.db.putWithTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMultimap.of(OneNodeDownTestSuite.CELL_1_2, Value.create(newValue, newTimestamp)));
        OneNodeDownGetTest.verifyTimestampAndValue(OneNodeDownTestSuite.CELL_1_2, newTimestamp, newValue);
    }

    @Test
    public void canMultiPut() {
        ImmutableMap<Cell, byte[]> entries = ImmutableMap.of(OneNodeDownTestSuite.CELL_2_1, newValue,
                OneNodeDownTestSuite.CELL_2_2, newValue);

        OneNodeDownTestSuite.db.multiPut(ImmutableMap.of(OneNodeDownTestSuite.TEST_TABLE, entries), newTimestamp);
        OneNodeDownGetTest.verifyTimestampAndValue(OneNodeDownTestSuite.CELL_2_1, newTimestamp, newValue);
        OneNodeDownGetTest.verifyTimestampAndValue(OneNodeDownTestSuite.CELL_2_2, newTimestamp, newValue);
    }

    @Test
    public void canPutUnlessExists() {
        expectException.expect(KeyAlreadyExistsException.class);
        OneNodeDownTestSuite.db.putUnlessExists(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_3_2, OneNodeDownTestSuite.DEFAULT_VALUE));
        OneNodeDownGetTest.verifyTimestampAndValue(OneNodeDownTestSuite.CELL_3_2, AtlasDbConstants.TRANSACTION_TS,
                OneNodeDownTestSuite.DEFAULT_VALUE);
    }

    @Test
    public void putUnlessExistsThrowsOnExists() {
        expectException.expect(KeyAlreadyExistsException.class);
        OneNodeDownTestSuite.db.putUnlessExists(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_VALUE));
    }

    @Test
    public void canAddGarbageCollectionSentinelValues() {
        OneNodeDownTestSuite.db.addGarbageCollectionSentinelValues(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableSet.of(OneNodeDownTestSuite.CELL_3_1));
        Map<Cell, Long> latestTimestamp = OneNodeDownTestSuite.db.getLatestTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_3_1, Long.MAX_VALUE));
        assertEquals(Value.INVALID_VALUE_TIMESTAMP,
                latestTimestamp.get(OneNodeDownTestSuite.CELL_3_1).longValue());
    }
}
