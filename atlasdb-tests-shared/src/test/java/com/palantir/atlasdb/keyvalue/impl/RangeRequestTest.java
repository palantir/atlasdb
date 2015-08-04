/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import org.junit.Assert;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;

public class RangeRequestTest {

    @Test
    public void testPrefix() {
        byte[] end = RangeRequest.builder().prefixRange(new byte[] {-1}).build().getEndExclusive();
        Assert.assertTrue(end.length == 0);
        end = RangeRequest.builder().prefixRange(new byte[] {-2}).build().getEndExclusive();
        Assert.assertTrue(end.length == 1);
        Assert.assertTrue(end[0] == -1);

        end = RangeRequest.builder().prefixRange(new byte[] {0, -1}).build().getEndExclusive();
        Assert.assertTrue(end.length == 1);
        Assert.assertTrue(end[0] == 1);

        end = RangeRequest.builder().prefixRange(new byte[] {0, -1, 0}).build().getEndExclusive();
        Assert.assertTrue(end.length == 3);
        Assert.assertTrue(end[2] == 1);
    }

}
