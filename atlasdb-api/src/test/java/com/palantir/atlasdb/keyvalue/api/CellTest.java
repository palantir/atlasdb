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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeNullPointerException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public final class CellTest {

    @Test
    public void testCreate() {
        Cell cell = Cell.create(bytes("row"), bytes("col"));
        assertThat(cell.getRowName()).isEqualTo(bytes("row"));
        assertThat(cell.getColumnName()).isEqualTo(bytes("col"));
        assertThatThrownBy(() -> Cell.create(null, bytes("col"))).isInstanceOf(SafeNullPointerException.class);
        assertThatThrownBy(() -> Cell.create(bytes("row"), null)).isInstanceOf(SafeNullPointerException.class);
        assertThatThrownBy(() -> Cell.create(bytes(""), bytes(""))).isInstanceOf(SafeIllegalArgumentException.class);
        assertThatThrownBy(() -> Cell.create(bytes("row"), bytes(""))).isInstanceOf(SafeIllegalArgumentException.class);
        assertThatThrownBy(() -> Cell.create(bytes(""), bytes("col"))).isInstanceOf(SafeIllegalArgumentException.class);
        assertThatThrownBy(() -> Cell.create(bytes("row"), bytes("x".repeat(Cell.MAX_NAME_LENGTH + 1))))
                .isInstanceOf(SafeIllegalArgumentException.class);
        assertThatThrownBy(() -> Cell.create(bytes("x".repeat(Cell.MAX_NAME_LENGTH + 1)), bytes("col")))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("ConstantConditions") // explicitly testing conditions
    public void testIsNameValid() {
        assertThat(Cell.isNameValid(bytes("row"))).isTrue();
        assertThat(Cell.isNameValid(null)).isFalse();
        assertThat(Cell.isNameValid(new byte[0])).isFalse();
        assertThat(Cell.isNameValid(bytes("x"))).isTrue();
        assertThat(Cell.isNameValid(bytes("x".repeat(Cell.MAX_NAME_LENGTH + 1))))
                .isFalse();
    }

    @Test
    public void testCompareTo() {
        assertThat(Cell.create(bytes("row"), bytes("col")))
                .isEqualByComparingTo(Cell.create(bytes("row"), bytes("col")));
        assertThat(Cell.create(bytes("row1"), bytes("col1")))
                .isNotEqualByComparingTo(Cell.create(bytes("row2"), bytes("col1")));
        assertThat(Cell.create(bytes("row1"), bytes("col1")))
                .isNotEqualByComparingTo(Cell.create(bytes("row1"), bytes("col2")));
        assertThat(Cell.create(bytes("row1"), bytes("col1"))).isLessThan(Cell.create(bytes("row2"), bytes("col1")));
        assertThat(Cell.create(bytes("row1"), bytes("col1"))).isLessThan(Cell.create(bytes("row1"), bytes("col2")));
    }

    @Test
    public void testEquals() {
        assertThat(Cell.create(bytes("row"), bytes("col"))).isEqualTo(Cell.create(bytes("row"), bytes("col")));
        assertThat(Cell.create(bytes("row"), bytes("col"))).isNotEqualTo(Cell.create(bytes("col"), bytes("row")));
    }

    @Test
    public void testHashCode() {
        assertThat(Cell.create(bytes("row"), bytes("col")).hashCode()).isNotZero();
        assertThat(Cell.create(bytes("row"), bytes("col")))
                .describedAs("Cell unfortunately has a non-ideal hashCode where swapped "
                        + "row and column values lead to the same hashCode and cannot be changed due "
                        + "to backward compatibility. See CellReference#goodHash")
                .hasSameHashCodeAs(Cell.create(bytes("col"), bytes("row")));
    }

    @Test
    public void testSizeInBytes() {
        assertThat(createCellWithByteSize(2).sizeInBytes()).isEqualTo(2);
        assertThat(createCellWithByteSize(63).sizeInBytes()).isEqualTo(63);
        assertThat(createCellWithByteSize(256).sizeInBytes()).isEqualTo(256);
    }

    private static Cell createCellWithByteSize(int size) {
        Preconditions.checkArgument(size >= 2, "Size should be at least 2");
        return Cell.create(new byte[size / 2], new byte[size - (size / 2)]);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
