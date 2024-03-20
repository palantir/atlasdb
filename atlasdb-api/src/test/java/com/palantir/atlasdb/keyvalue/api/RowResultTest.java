/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.primitives.UnsignedBytes;
import java.lang.reflect.Field;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

public class RowResultTest {

    @Test
    public void testNullColumnsEqual() {
        RowResult<byte[]> left = rowResultNullColumns(bytes(1));
        RowResult<byte[]> right = rowResultNullColumns(bytes(1));
        assertEquals(left, right);
    }

    @Test
    public void testNullColumnsNotEqual() {
        RowResult<byte[]> left = rowResultNullColumns(bytes(1));
        RowResult<byte[]> right = rowResultNullColumns(bytes(2));
        assertNotEquals(left, right);
    }

    @Test
    public void testOneNullColumnRowsEqual() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        RowResult<byte[]> right = rowResultNullColumns(bytes(1));
        assertNotEquals(left, right);
    }

    @Test
    public void testOneNullColumnRowsNotEqual() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        RowResult<byte[]> right = rowResultNullColumns(bytes(2));
        assertNotEquals(left, right);
    }

    @Test
    public void testRowResultEquals() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        RowResult<byte[]> right = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        assertEquals(left, right);
    }

    @Test
    public void testRowResultRowNameNotEquals() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        RowResult<byte[]> right = RowResult.of(Cell.create(bytes(4), bytes(2)), bytes(3));
        assertNotEquals(left, right);
    }

    @Test
    public void testRowResultColumnNameNotEquals() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        RowResult<byte[]> right = RowResult.of(Cell.create(bytes(1), bytes(4)), bytes(3));
        assertNotEquals(left, right);
    }

    @Test
    public void testRowResultColumnValueNotEquals() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        RowResult<byte[]> right = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(4));
        assertNotEquals(left, right);
    }

    @Test
    public void testRowResultColumnLengthNotEqual() {
        RowResult<byte[]> left = RowResult.of(Cell.create(bytes(1), bytes(2)), bytes(3));
        TreeMap<byte[], byte[]> rightColumns = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        rightColumns.put(bytes(2), bytes(3));
        rightColumns.put(bytes(4), bytes(5));
        RowResult<byte[]> right = RowResult.create(bytes(1), rightColumns);
        assertNotEquals(left, right);
    }

    private static RowResult<byte[]> rowResultNullColumns(byte[] row) {
        RowResult<byte[]> rowResult = RowResult.of(Cell.create(row, bytes(1)), new byte[] {});
        try {
            Field columns = rowResult.getClass().getDeclaredField("columns");
            columns.setAccessible(true);
            columns.set(rowResult, null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return rowResult;
    }

    private static byte[] bytes(int val) {
        return new byte[] {(byte) val};
    }

    private static void assertEquals(Object left, Object right) {
        assertThat(left.equals(right)).isEqualTo(true);
        assertThat(right.equals(left)).isEqualTo(true);
    }

    private static void assertNotEquals(Object left, Object right) {
        assertThat(left.equals(right)).isEqualTo(false);
        assertThat(right.equals(left)).isEqualTo(false);
    }
}
