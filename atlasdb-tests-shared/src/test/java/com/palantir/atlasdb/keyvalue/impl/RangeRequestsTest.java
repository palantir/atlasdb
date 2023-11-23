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

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import java.util.Random;
import org.junit.Test;

/* TODO(boyoruk): Migrate to JUnit5 */
public class RangeRequestsTest {

    private static final byte[] BYTES_1 = PtBytes.toBytes("apple");
    private static final byte[] BYTES_2 = PtBytes.toBytes("banana");
    private static final byte MAXIMAL_BYTE = (byte) 0xff;

    private Random random = new Random();

    @Test
    public void testNextAndPrev() {
        assertNextPrevEqualsOrig(new byte[] {0});
        assertNextPrevEqualsOrig(new byte[] {MAXIMAL_BYTE});

        for (int i = 0; i < 100000; i++) {
            assertNextPrevEqualsOrig(generateRandomWithFreqLogLen());
        }
    }

    @Test
    public void unboundedRangeBothEndsIsContiguous() {
        assertThat(RangeRequests.isContiguousRange(true, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY))
                .isTrue();
        assertThat(RangeRequests.isContiguousRange(false, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY))
                .isTrue();
    }

    @Test
    public void forwardContiguousRangeShouldStartBeforeEnd() {
        assertThat(RangeRequests.isContiguousRange(false, BYTES_1, BYTES_2)).isTrue();
        assertThat(RangeRequests.isContiguousRange(false, BYTES_2, BYTES_1)).isFalse();
    }

    @Test
    public void reverseContiguousRangeShouldEndBeforeStart() {
        assertThat(RangeRequests.isContiguousRange(true, BYTES_1, BYTES_2)).isFalse();
        assertThat(RangeRequests.isContiguousRange(true, BYTES_2, BYTES_1)).isTrue();
    }

    @Test
    public void emptyRangesAreContiguous() {
        assertThat(RangeRequests.isContiguousRange(false, BYTES_1, BYTES_1)).isTrue();
        assertThat(RangeRequests.isContiguousRange(true, BYTES_1, BYTES_1)).isTrue();
    }

    @Test
    public void rangesUnboundedOnOneEndAreContiguous() {
        assertThat(RangeRequests.isContiguousRange(false, BYTES_1, PtBytes.EMPTY_BYTE_ARRAY))
                .isTrue();
        assertThat(RangeRequests.isContiguousRange(true, BYTES_1, PtBytes.EMPTY_BYTE_ARRAY))
                .isTrue();
        assertThat(RangeRequests.isContiguousRange(false, PtBytes.EMPTY_BYTE_ARRAY, BYTES_1))
                .isTrue();
        assertThat(RangeRequests.isContiguousRange(true, PtBytes.EMPTY_BYTE_ARRAY, BYTES_1))
                .isTrue();
    }

    @Test
    public void unboundedRangeBothEndsIsNotEmpty() {
        assertThat(RangeRequests.isExactlyEmptyRange(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY))
                .isFalse();
    }

    @Test
    public void unboundedRangeOneEndIsNotEmpty() {
        assertThat(RangeRequests.isExactlyEmptyRange(PtBytes.EMPTY_BYTE_ARRAY, BYTES_1))
                .isFalse();
        assertThat(RangeRequests.isExactlyEmptyRange(BYTES_2, PtBytes.EMPTY_BYTE_ARRAY))
                .isFalse();
    }

    @Test
    public void rangeWithDifferentBoundsNotEmpty() {
        assertThat(RangeRequests.isExactlyEmptyRange(BYTES_1, BYTES_2)).isFalse();
        assertThat(RangeRequests.isExactlyEmptyRange(BYTES_2, BYTES_1)).isFalse();
    }

    @Test
    public void rangeWithSameBoundIsEmpty() {
        assertThat(RangeRequests.isExactlyEmptyRange(BYTES_1, BYTES_1)).isTrue();
        assertThat(RangeRequests.isExactlyEmptyRange(BYTES_2, BYTES_2)).isTrue();
    }

    @Test
    public void endNameForPrefixScanOfEmptyByteArrayIsEmpty() {
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {})).isEmpty();
    }

    @Test
    public void endNameForPrefixScanOfSingleByteArraysIncrementsTheByte() {
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {0})).isEqualTo(new byte[] {1});
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {42})).isEqualTo(new byte[] {43});
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {127})).isEqualTo(new byte[] {-128});
    }

    @Test
    public void endNameForPrefixScanOfMultiByteArraysWithoutOverflowIncrementsTheLeastSignificantByte() {
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {0, 1, 2}))
                .isEqualTo(new byte[] {0, 1, 3});
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {9, 8, 88}))
                .isEqualTo(new byte[] {9, 8, 89});
    }

    @Test
    public void endNameForPrefixScanOfArrayOfMaximalValuedBytesIsEmpty() {
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {MAXIMAL_BYTE}))
                .isEmpty();
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {MAXIMAL_BYTE, MAXIMAL_BYTE, MAXIMAL_BYTE}))
                .isEmpty();
    }

    @Test
    public void endNameForPrefixScanOfArrayEndingInMaximalValuedBytesIncrementsLastNonMaximalByteAndCollapses() {
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {2, 3, MAXIMAL_BYTE}))
                .isEqualTo(new byte[] {2, 4});
        assertThat(RangeRequests.createEndNameForPrefixScan(new byte[] {MAXIMAL_BYTE, 3, MAXIMAL_BYTE, MAXIMAL_BYTE}))
                .isEqualTo(new byte[] {MAXIMAL_BYTE, 4});
        assertThat(RangeRequests.createEndNameForPrefixScan(
                        new byte[] {2, MAXIMAL_BYTE, MAXIMAL_BYTE, 3, MAXIMAL_BYTE, MAXIMAL_BYTE}))
                .isEqualTo(new byte[] {2, MAXIMAL_BYTE, MAXIMAL_BYTE, 4});
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
        assertThat(value)
                .isEqualTo(RangeRequests.nextLexicographicName(RangeRequests.previousLexicographicName(value)));
        assertThat(value)
                .isEqualTo(RangeRequests.previousLexicographicName(RangeRequests.nextLexicographicName(value)));
    }
}
