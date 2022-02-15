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

package com.palantir.atlasdb.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.junit.Test;

public final class ByteArrayUtilitiesTests {

    @Test
    public void getRowsCompareAllEntries() {
        NavigableMap<byte[], RowResult<byte[]>> first = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        NavigableMap<byte[], RowResult<byte[]>> second = new TreeMap<>(UnsignedBytes.lexicographicalComparator());

        // These are *not* extracted into common variables to make sure that we are comparing based on the values,
        // not the references.
        first.put(new byte[] {1, 2, 3, 4}, RowResult.of(Cell.create(new byte[] {5, 6}, new byte[] {7, 8}), new byte[] {9
        }));
        second.put(
                new byte[] {1, 2, 3, 4},
                RowResult.of(Cell.create(new byte[] {5, 6}, new byte[] {7, 8}), new byte[] {9}));
        first.put(
                new byte[] {1, 2, 3, 5},
                RowResult.of(Cell.create(new byte[] {5, 6}, new byte[] {7, 8}), new byte[] {111}));
        second.put(
                new byte[] {1, 2, 3, 5},
                RowResult.of(Cell.create(new byte[] {5, 6}, new byte[] {7, 8}), new byte[] {9}));
        assertThat(ByteArrayUtilities.areRowResultsEqual(first, second)).isFalse();
    }
}
