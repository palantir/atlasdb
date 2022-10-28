/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import java.util.Arrays;
import org.junit.Test;

public class ValueTest {
    private static final int SMALLER = 100;
    private static final int LARGER = 200;
    private static final byte BYTE = (byte) 0xa;

    @Test
    public void sizeInBytesOfValueWithNoContentsIsSizeOfLong() {
        assertThat(Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP)
                        .sizeInBytes())
                .isEqualTo(Long.BYTES);
    }

    @Test
    public void sizeInBytesOfValueOrderFollowsContentsSizeOrder() {
        assertThat(Value.create(spawnBytes(SMALLER), Value.INVALID_VALUE_TIMESTAMP)
                        .sizeInBytes())
                .isLessThan(Value.create(spawnBytes(LARGER), Value.INVALID_VALUE_TIMESTAMP)
                        .sizeInBytes());
    }

    private static byte[] spawnBytes(int size) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, BYTE);
        return bytes;
    }
}
