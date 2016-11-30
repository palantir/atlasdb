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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;

public class OneNodeDownPutTest {

    private static final byte[] newContents = PtBytes.toBytes("new_value");
    private static final long newTimestamp = 7L;

    private static final Value newValue = Value.create(newContents, newTimestamp);

    @Test
    public void canPut() {
        OneNodeDownTestSuite.db.put(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_1_1, newContents), newTimestamp);
        OneNodeDownTestSuite.verifyValue(OneNodeDownTestSuite.CELL_1_1, newValue);
    }

    @Test
    public void canPutWithTimestamps() {
        OneNodeDownTestSuite.db.putWithTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMultimap.of(OneNodeDownTestSuite.CELL_1_2, newValue));
        OneNodeDownTestSuite.verifyValue(OneNodeDownTestSuite.CELL_1_2, newValue);
    }

    @Test
    public void canMultiPut() {
        ImmutableMap<Cell, byte[]> entries = ImmutableMap.of(
                OneNodeDownTestSuite.CELL_2_1, newContents,
                OneNodeDownTestSuite.CELL_2_2, newContents);

        OneNodeDownTestSuite.db.multiPut(ImmutableMap.of(OneNodeDownTestSuite.TEST_TABLE, entries), newTimestamp);
        OneNodeDownTestSuite.verifyValue(OneNodeDownTestSuite.CELL_2_1, newValue);
        OneNodeDownTestSuite.verifyValue(OneNodeDownTestSuite.CELL_2_2, newValue);
    }

    @Test
    public void canPutUnlessExists() {
        OneNodeDownTestSuite.db.putUnlessExists(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_4_1, OneNodeDownTestSuite.DEFAULT_CONTENTS));
        OneNodeDownTestSuite.verifyValue(OneNodeDownTestSuite.CELL_4_1,
                Value.create(OneNodeDownTestSuite.DEFAULT_CONTENTS, AtlasDbConstants.TRANSACTION_TS));
    }

    @Test
    public void putUnlessExistsThrowsOnExists() {
        assertThatThrownBy(() -> OneNodeDownTestSuite.db.putUnlessExists(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_CONTENTS))).isInstanceOf(
                KeyAlreadyExistsException.class);
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
