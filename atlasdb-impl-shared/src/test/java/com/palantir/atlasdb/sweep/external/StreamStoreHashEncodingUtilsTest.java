/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.atlasdb.table.description.ValueType;

public class StreamStoreHashEncodingUtilsTest {
    private static final GenericStreamIdentifier IDENTIFIER = ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG,
            ValueType.FIXED_LONG.convertFromJava(1L));

    private static final byte[] ENCODED_IDENTIFIER = {-114, -58, -91, 70, 112, 92, 76, 40};
    private static final byte[] ENCODED_IDENTIFIER_AND_BLOCK_ONE = {-35, 121, 15, -77, -86, -93, 113, -107};

    @Test
    public void getZeroHashComponentsReturnsEmptyByteArray() {
        assertThat(StreamStoreHashEncodingUtils.getHashComponentWithoutBlockId(0, IDENTIFIER)).isEmpty();
        assertThat(StreamStoreHashEncodingUtils.getHashComponent(0, IDENTIFIER, 1)).isEmpty();
    }

    @Test
    public void canGetHashComponentWithoutBlockId() {
        assertThat(StreamStoreHashEncodingUtils.getHashComponentWithoutBlockId(1, IDENTIFIER))
                .isEqualTo(ENCODED_IDENTIFIER);
    }

    @Test
    public void getHashComponentWithoutBlockIdComputesBasedOnOneElement() {
        assertThat(StreamStoreHashEncodingUtils.getHashComponentWithoutBlockId(2, IDENTIFIER))
                .isEqualTo(ENCODED_IDENTIFIER);
    }

    @Test
    public void canGetHashComponentWithBlockId() {
        assertThat(StreamStoreHashEncodingUtils.getHashComponent(2, IDENTIFIER, 1))
                .isEqualTo(ENCODED_IDENTIFIER_AND_BLOCK_ONE);
    }

    @Test
    public void getHashComponentWithBlockIdIgnoresItIfOnlyOneHashComponentSpecified() {
        assertThat(StreamStoreHashEncodingUtils.getHashComponent(1, IDENTIFIER, 1))
                .isEqualTo(ENCODED_IDENTIFIER); // same as getHashComponentWithoutBlockId on the identifier
    }

    @Test
    public void blockIdInfluencesHashComponent() {
        // Technically this test is a bit over-strict
        assertThat(StreamStoreHashEncodingUtils.getHashComponent(2, IDENTIFIER, 1))
                .isNotEqualTo(StreamStoreHashEncodingUtils.getHashComponent(2, IDENTIFIER, 2));
    }

    @Test
    public void throwsIfRequestingNegativeNumberOfHashComponents() {
        assertThatThrownBy(() -> StreamStoreHashEncodingUtils.getHashComponentWithoutBlockId(-6, IDENTIFIER))
                .isInstanceOf(IllegalStateException.class);
    }
}
