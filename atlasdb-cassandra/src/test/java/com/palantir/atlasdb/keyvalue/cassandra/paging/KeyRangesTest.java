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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.thrift.KeyRange;
import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;

public class KeyRangesTest {
    @Test
    public void createKeyRangeWithExplicitEnd() {
        byte[] start = PtBytes.toBytes("start");
        byte[] end = PtBytes.toBytes("the-end");
        byte[] enc = RangeRequests.previousLexicographicName(end);
        int limit = 7;
        KeyRange keyRange = KeyRanges.createKeyRange(start, end, limit);

        assertArrayEquals(start, keyRange.getStart_key());
        assertArrayEquals(enc, keyRange.getEnd_key());
        assertEquals(limit, keyRange.getCount());
    }

    @Test
    public void createKeyRangeWithEmptyEnd() {
        byte[] start = PtBytes.toBytes("start");
        byte[] end = PtBytes.toBytes("");
        int limit = 7;
        KeyRange keyRange = KeyRanges.createKeyRange(start, end, limit);

        assertArrayEquals(start, keyRange.getStart_key());
        assertArrayEquals(end, keyRange.getEnd_key());
        assertEquals(limit, keyRange.getCount());
    }
}
