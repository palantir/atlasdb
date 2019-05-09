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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;

public class RangeRequestsTest {

    private static final byte[] BYTES_1 = PtBytes.toBytes("apple");
    private static final byte[] BYTES_2 = PtBytes.toBytes("banana");

    private Random random = new Random();

    @Test
    public void testNextAndPrev() {
        assertNextPrevEqualsOrig(new byte[] {0});
        assertNextPrevEqualsOrig(new byte[] {(byte) 0xff});

        for (int i = 0; i < 100000; i++) {
            assertNextPrevEqualsOrig(generateRandomWithFreqLogLen());
        }
    }

    @Test
    public void unboundedRangeBothEndsIsValid() {
        assertThat(RangeRequests.isValidRange(
                true, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY)).isTrue();
        assertThat(RangeRequests.isValidRange(
                false, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY)).isTrue();
    }

    @Test
    public void forwardRangeShouldStartBeforeEnd() {
        assertThat(RangeRequests.isValidRange(false, BYTES_1, BYTES_2)).isTrue();
        assertThat(RangeRequests.isValidRange(false, BYTES_2, BYTES_1)).isFalse();
    }

    @Test
    public void reverseRangeShouldEndBeforeStart() {
        assertThat(RangeRequests.isValidRange(true, BYTES_1, BYTES_2)).isFalse();
        assertThat(RangeRequests.isValidRange(true, BYTES_2, BYTES_1)).isTrue();
    }

    @Test
    public void emptyRangesAreValid() {
        assertThat(RangeRequests.isValidRange(false, BYTES_1, BYTES_1)).isTrue();
        assertThat(RangeRequests.isValidRange(true, BYTES_1, BYTES_1)).isTrue();
    }

    @Test
    public void rangesUnboundedOnOneEndAreValid() {
        assertThat(RangeRequests.isValidRange(false, BYTES_1, PtBytes.EMPTY_BYTE_ARRAY)).isTrue();
        assertThat(RangeRequests.isValidRange(true, BYTES_1, PtBytes.EMPTY_BYTE_ARRAY)).isTrue();
        assertThat(RangeRequests.isValidRange(false, PtBytes.EMPTY_BYTE_ARRAY, BYTES_1)).isTrue();
        assertThat(RangeRequests.isValidRange(true, PtBytes.EMPTY_BYTE_ARRAY, BYTES_1)).isTrue();
    }

    private byte[] generateRandomWithFreqLogLen() {
        long randomLong = random.nextLong();
        // lg(n) distribution of len
        int len = Long.numberOfTrailingZeros(randomLong) + 1;
        byte[] ret = new byte[len];
        random.nextBytes(ret);
        return ret;
    }

    private void assertNextPrevEqualsOrig(byte[] value) {
        Assert.assertTrue(Arrays.equals(value,
                RangeRequests.nextLexicographicName(RangeRequests.previousLexicographicName(value))));
        Assert.assertTrue(Arrays.equals(value,
                RangeRequests.previousLexicographicName(RangeRequests.nextLexicographicName(value))));
    }
}
