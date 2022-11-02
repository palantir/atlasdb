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

import org.junit.Test;

public class ValueTest {
    private static final int CONTENTS_SIZE_1 = 100;
    private static final int CONTENTS_SIZE_2 = 200;

    @Test
    public void sizeInBytesOfValueWithNoContentsIsSizeOfLong() {
        assertThat(createValue(0).sizeInBytes()).isEqualTo(Long.BYTES);
    }

    @Test
    public void sizeInBytesIsCorrectForOneByteContentsArray() {
        assertThat(createValue(1).sizeInBytes()).isEqualTo(1L + Long.BYTES);
    }

    @Test
    public void sizeInBytesIsCorrectForMultipleBytesArray() {
        assertThat(createValue(CONTENTS_SIZE_1).sizeInBytes()).isEqualTo(Long.sum(CONTENTS_SIZE_1, Long.BYTES));
        assertThat(createValue(CONTENTS_SIZE_2).sizeInBytes()).isEqualTo(Long.sum(CONTENTS_SIZE_2, Long.BYTES));
    }

    private static Value createValue(int contentsSize) {
        return Value.create(spawnBytes(contentsSize), Value.INVALID_VALUE_TIMESTAMP);
    }

    private static byte[] spawnBytes(int size) {
        return new byte[size];
    }
}
