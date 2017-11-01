/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;

public class RangeRequestTest {
    private static final byte[] START = PtBytes.toBytes("commencement");
    private static final byte[] END = PtBytes.toBytes("terminus");
    private static final RangeRequest BOUNDED_REQUEST = RangeRequest.builder()
            .startRowInclusive(START)
            .endRowExclusive(END)
            .build();

    @Test
    public void testPrefix() {
        byte[] endExclusive = RangeRequest.builder().prefixRange(new byte[] {-1}).build().getEndExclusive();
        assertEquals(0, endExclusive.length);
        endExclusive = RangeRequest.builder().prefixRange(new byte[] {-2}).build().getEndExclusive();
        assertEquals(1, endExclusive.length);
        assertEquals(-1, endExclusive[0]);

        endExclusive = RangeRequest.builder().prefixRange(new byte[] {0, -1}).build().getEndExclusive();
        assertEquals(1, endExclusive.length);
        assertEquals(1, endExclusive[0]);

        endExclusive = RangeRequest.builder().prefixRange(new byte[] {0, -1, 0}).build().getEndExclusive();
        assertEquals(3, endExclusive.length);
        assertEquals(1, endExclusive[2]);
    }

    @Test
    public void testEmpty() {
        RangeRequest emptyRequest = RangeRequest.builder().endRowExclusive(RangeRequests.getFirstRowName()).build();
        assertTrue(emptyRequest.isEmptyRange());
        emptyRequest = RangeRequest.reverseBuilder().endRowExclusive(RangeRequests.getLastRowName()).build();
        assertTrue(emptyRequest.isEmptyRange());
    }

    @Test
    public void testValueInBoundedRange() {
        assertTrue(BOUNDED_REQUEST.inRange(PtBytes.toBytes("middle")));
    }

    @Test
    public void testValueBeforeBoundedRange() {
        assertFalse(BOUNDED_REQUEST.inRange(PtBytes.toBytes("an early value")));
    }

    @Test
    public void testValueAfterBoundedRange() {
        assertFalse(BOUNDED_REQUEST.inRange(PtBytes.toBytes("too late")));
    }

    @Test
    public void testStartIsInclusive() {
        assertTrue(BOUNDED_REQUEST.inRange(START));
    }

    @Test
    public void testEndIsExclusive() {
        assertFalse(BOUNDED_REQUEST.inRange(END));
    }

    @Test
    public void testValueBeforeRangeBoundedByStartOnly() {
        RangeRequest request = RangeRequest.builder().startRowInclusive(START).build();
        assertFalse(request.inRange(PtBytes.toBytes("an early value")));
    }

    @Test
    public void testValueAfterRangeBoundedByStartOnly() {
        RangeRequest request = RangeRequest.builder().startRowInclusive(START).build();
        assertTrue(request.inRange(END));
    }
}
