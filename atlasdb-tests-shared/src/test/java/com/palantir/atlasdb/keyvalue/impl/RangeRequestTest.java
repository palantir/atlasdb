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
package com.palantir.atlasdb.keyvalue.impl;

import static org.junit.Assert.assertEquals;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import org.junit.Assert;
import org.junit.Test;

public class RangeRequestTest {

    @Test
    public void testPrefix() {
        byte[] end = RangeRequest.builder().prefixRange(new byte[] {-1}).build().getEndExclusive();
        assertEquals(0, end.length);
        end = RangeRequest.builder().prefixRange(new byte[] {-2}).build().getEndExclusive();
        assertEquals(1, end.length);
        assertEquals(-1, end[0]);

        end = RangeRequest.builder().prefixRange(new byte[] {0, -1}).build().getEndExclusive();
        assertEquals(1, end.length);
        assertEquals(1, end[0]);

        end = RangeRequest.builder().prefixRange(new byte[] {0, -1, 0}).build().getEndExclusive();
        assertEquals(3, end.length);
        assertEquals(1, end[2]);
    }

    @Test
    public void testEmpty() {
        RangeRequest request = RangeRequest.builder().endRowExclusive(RangeRequests.getFirstRowName()).build();
        Assert.assertTrue(request.isEmptyRange());
        request = RangeRequest.reverseBuilder().endRowExclusive(RangeRequests.getLastRowName()).build();
        Assert.assertTrue(request.isEmptyRange());
    }

}
