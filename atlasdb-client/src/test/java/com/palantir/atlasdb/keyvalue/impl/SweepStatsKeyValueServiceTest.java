/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.timestamp.TimestampService;

public class SweepStatsKeyValueServiceTest {
    private static final byte[] ROW = "row".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COLUMN_1 = "column1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COLUMN_2 = "column2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);
    private static final TableReference TABLE = TableReference.createWithEmptyNamespace("table");

    private KeyValueService delegate = mock(KeyValueService.class);

    private SweepStatsKeyValueService kvs;

    @Before
    public void before() {
        TimestampService timestampService = mock(TimestampService.class);
        kvs = SweepStatsKeyValueService.create(delegate, timestampService,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_THRESHOLD,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_SIZE_THRESHOLD
        );
    }

    @Test
    public void delegatesInitializationCheck() {
        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(kvs.isInitialized());
        assertTrue(kvs.isInitialized());
    }

    @Test
    public void putShouldRecordModifications() {
        Map<Cell, byte[]> twoValues = ImmutableMap.of(Cell.create(ROW, COLUMN_1), VALUE,
                                                      Cell.create(ROW, COLUMN_2), VALUE);
        kvs.put(TABLE, twoValues, 100L);

        assertEquals(2, kvs.getTotalModifications());
        assertEquals(10L, kvs.getTotalModificationsSize());
        assertEquals(2, kvs.getWriteCount(TABLE));
    }

    @Test
    public void deleteRangeAllCountsAsClearingTheTable() throws Exception {
        kvs.deleteRange(TABLE, RangeRequest.all());
        assertTrue(kvs.hasBeenCleared(TABLE));
    }

    @Test
    public void otherDeleteRangeDoesNotCountAsClearingTheTable() throws Exception {
        RangeRequest request = RangeRequest.builder().startRowInclusive(ROW).build();
        kvs.deleteRange(TABLE, request);
        assertFalse(kvs.hasBeenCleared(TABLE));
    }
}
