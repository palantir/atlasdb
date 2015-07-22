// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.RangeRequests;

public class RangeRequestsTest {

    @Test
    public void testNextAndPrev() {
        assertNextPrevEqualsOrig(new byte[] {0});
        assertNextPrevEqualsOrig(new byte[] {(byte) 0xff});

        for (int i = 0 ; i < 100000 ; i++) {
            assertNextPrevEqualsOrig(generateRandomWithFreqLogLen());
        }
    }

    Random r = new Random();

    private byte[] generateRandomWithFreqLogLen() {
        long l = r.nextLong();
        // lg(n) distrobution of len
        int len = Long.numberOfTrailingZeros(l) + 1;
        byte[] ret = new byte[len];
        r.nextBytes(ret);
        return ret;
    }

    private void assertNextPrevEqualsOrig(byte[] x) {
        Assert.assertTrue(Arrays.equals(x, RangeRequests.nextLexicographicName(RangeRequests.previousLexicographicName(x))));
        Assert.assertTrue(Arrays.equals(x, RangeRequests.previousLexicographicName(RangeRequests.nextLexicographicName(x))));
    }
}
