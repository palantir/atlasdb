/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import org.junit.Test;

public class ColumnRangeSelectionTest {
    private static final byte[] BYTES_1 = PtBytes.toBytes("aaaaaaa");
    private static final byte[] BYTES_2 = PtBytes.toBytes("bbbbbbb");

    @Test
    public void canCreateUnboundedColumnRangeSelection() {
        assertThatCode(() -> new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY))
                .doesNotThrowAnyException();
        assertThatCode(() -> new ColumnRangeSelection(null, null)).doesNotThrowAnyException();
    }

    @Test
    public void canCreateWithStartBeforeEnd() {
        assertThatCode(() -> new ColumnRangeSelection(BYTES_1, BYTES_2)).doesNotThrowAnyException();
    }

    @Test
    public void cannotCreateWithEndBeforeStart() {
        assertThatThrownBy(() -> new ColumnRangeSelection(BYTES_2, BYTES_1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not form a valid range");
    }

    @Test
    public void canCreateUnboundedOneEnd() {
        assertThatCode(() -> new ColumnRangeSelection(BYTES_1, PtBytes.EMPTY_BYTE_ARRAY))
                .doesNotThrowAnyException();
        assertThatCode(() -> new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, BYTES_2))
                .doesNotThrowAnyException();
    }

    @Test
    public void canCreateEmptyRange() {
        assertThatThrownBy(() -> new ColumnRangeSelection(BYTES_1, BYTES_1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not form a valid range");
    }
}
