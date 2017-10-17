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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.cassandra.thrift.Column;
import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;

public class SingleRowColumnPagerTest {

    @Test
    public void startShouldBeEmptyWhenThereIsNoLastSeenColumn() {
        assertEquals(
                Optional.of(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY)),
                SingleRowColumnPager.getStartColumn(null));
    }

    @Test
    public void startShouldBeOneBelowPreviousTimestamp() {
        assertEquals(
                Optional.of(CassandraKeyValueServices.makeCompositeBuffer(new byte[] { 1, 2, 3 }, 999L)),
                SingleRowColumnPager.getStartColumn(new Column(
                        CassandraKeyValueServices.makeCompositeBuffer(new byte[] { 1, 2, 3 }, 1000L))));
    }

    @Test
    public void startShouldRollOverToNextColumnNameWhenMinimumTimestampReached() {
        assertEquals(
                Optional.of(CassandraKeyValueServices.makeCompositeBuffer(
                        RangeRequests.nextLexicographicName(new byte[] {1, 2, 3 }), Long.MAX_VALUE)),
                SingleRowColumnPager.getStartColumn(new Column(
                        CassandraKeyValueServices.makeCompositeBuffer(new byte[] { 1, 2, 3 }, Long.MIN_VALUE))));
    }

    @Test
    public void startShouldBeEmptyWhenLastPossiblePointReached() {
        assertEquals(
                Optional.empty(),
                SingleRowColumnPager.getStartColumn(new Column(
                        CassandraKeyValueServices.makeCompositeBuffer(lastPossibleColumnName(), Long.MIN_VALUE))));
    }

    private static byte[] lastPossibleColumnName() {
        byte[] ret = new byte[Cell.MAX_NAME_LENGTH];
        for (int i = 0; i < Cell.MAX_NAME_LENGTH; ++i) {
            ret[i] |= 0xff;
        }
        return ret;
    }

}
