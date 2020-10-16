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

package com.palantir.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Range;
import org.junit.Test;

public class AtlasLockDescriptorRangesTest {
    private static final String TABLE = "test.table";
    private static final String TABLE_2 = "other.table";
    private static final byte[] BYTES = new byte[] {1, 2, 3};
    private static final byte[] BYTES_SUFFIX = new byte[] {1, 2, 3, 4, 5};
    private static final byte[] OTHER_BYTES = new byte[] {3, 6, 3, 7};

    @Test
    public void fullTableRangeContainsRowAndCellDescriptors() {
        Range<LockDescriptor> fullTable = AtlasLockDescriptorRanges.fullTable(TABLE);

        assertThat(fullTable.contains(AtlasRowLockDescriptor.of(TABLE, BYTES))).isTrue();
        assertThat(fullTable.contains(AtlasCellLockDescriptor.of(TABLE, BYTES, BYTES_SUFFIX)))
                .isTrue();
    }

    @Test
    public void fullTableRangeDoesNotContainOtherTableDescriptors() {
        Range<LockDescriptor> fullTable = AtlasLockDescriptorRanges.fullTable(TABLE);

        assertThat(fullTable.contains(AtlasRowLockDescriptor.of(TABLE_2, BYTES)))
                .isFalse();
        assertThat(fullTable.contains(AtlasCellLockDescriptor.of(TABLE_2, BYTES, BYTES_SUFFIX)))
                .isFalse();
    }

    @Test
    public void rowPrefixRangeContainsRowAndCellDescriptorsForRow() {
        Range<LockDescriptor> rowPrefix = AtlasLockDescriptorRanges.rowPrefix(TABLE, BYTES);

        assertThat(rowPrefix.contains(AtlasRowLockDescriptor.of(TABLE, BYTES))).isTrue();
        assertThat(rowPrefix.contains(AtlasCellLockDescriptor.of(TABLE, BYTES, BYTES_SUFFIX)))
                .isTrue();
    }

    @Test
    public void rowPrefixRangeContainsRowAndCellDescriptorsForRowSuffix() {
        Range<LockDescriptor> rowPrefix = AtlasLockDescriptorRanges.rowPrefix(TABLE, BYTES);

        assertThat(rowPrefix.contains(AtlasRowLockDescriptor.of(TABLE, BYTES_SUFFIX)))
                .isTrue();
        assertThat(rowPrefix.contains(AtlasCellLockDescriptor.of(TABLE, BYTES_SUFFIX, OTHER_BYTES)))
                .isTrue();
    }

    @Test
    public void rowPrefixRangeDoesNotContainRowAndCellDescriptorsForOtherRows() {
        Range<LockDescriptor> rowPrefix = AtlasLockDescriptorRanges.rowPrefix(TABLE, BYTES);

        assertThat(rowPrefix.contains(AtlasRowLockDescriptor.of(TABLE, OTHER_BYTES)))
                .isFalse();
        assertThat(rowPrefix.contains(AtlasCellLockDescriptor.of(TABLE, OTHER_BYTES, OTHER_BYTES)))
                .isFalse();
    }

    @Test
    public void rowRangeContainsRowAndCellDescriptorsInRange() {
        Range<LockDescriptor> rowRange = AtlasLockDescriptorRanges.rowRange(TABLE, BYTES, OTHER_BYTES);

        assertThat(rowRange.contains(AtlasRowLockDescriptor.of(TABLE, BYTES))).isTrue();
        assertThat(rowRange.contains(AtlasCellLockDescriptor.of(TABLE, BYTES, BYTES_SUFFIX)))
                .isTrue();
        assertThat(rowRange.contains(AtlasRowLockDescriptor.of(TABLE, BYTES_SUFFIX)))
                .isTrue();
        assertThat(rowRange.contains(AtlasCellLockDescriptor.of(TABLE, BYTES_SUFFIX, OTHER_BYTES)))
                .isTrue();
    }

    @Test
    public void rowRangeDoesNotContainEndRowOrCellDescriptors() {
        Range<LockDescriptor> rowRange = AtlasLockDescriptorRanges.rowRange(TABLE, BYTES, OTHER_BYTES);

        assertThat(rowRange.contains(AtlasRowLockDescriptor.of(TABLE, OTHER_BYTES)))
                .isFalse();
        assertThat(rowRange.contains(AtlasCellLockDescriptor.of(TABLE, OTHER_BYTES, BYTES_SUFFIX)))
                .isFalse();
    }

    @Test
    public void exactRowContainsRowDescriptor() {
        Range<LockDescriptor> exactRow = AtlasLockDescriptorRanges.exactRow(TABLE, BYTES);

        assertThat(exactRow.contains(AtlasRowLockDescriptor.of(TABLE, BYTES))).isTrue();
    }

    @Test
    public void exactRowDoesNotContainCellDescriptorForRow() {
        Range<LockDescriptor> exactRow = AtlasLockDescriptorRanges.exactRow(TABLE, BYTES);

        assertThat(exactRow.contains(AtlasCellLockDescriptor.of(TABLE, BYTES, BYTES_SUFFIX)))
                .isFalse();
    }

    @Test
    public void exactRowDoesNotContainRowDescriptorForOtherRows() {
        Range<LockDescriptor> exactRow = AtlasLockDescriptorRanges.exactRow(TABLE, BYTES);

        assertThat(exactRow.contains(AtlasRowLockDescriptor.of(TABLE, BYTES_SUFFIX)))
                .isFalse();
        assertThat(exactRow.contains(AtlasRowLockDescriptor.of(TABLE, OTHER_BYTES)))
                .isFalse();
    }

    @Test
    public void exactCellContainsCellDescriptor() {
        Range<LockDescriptor> exactCell = AtlasLockDescriptorRanges.exactCell(TABLE, BYTES, BYTES);

        assertThat(exactCell.contains(AtlasCellLockDescriptor.of(TABLE, BYTES, BYTES)))
                .isTrue();
    }

    @Test
    public void exactCellDoesNotContainRowDescriptors() {
        Range<LockDescriptor> exactCell = AtlasLockDescriptorRanges.exactCell(TABLE, BYTES, BYTES);

        assertThat(exactCell.contains(AtlasRowLockDescriptor.of(TABLE, BYTES))).isFalse();
        assertThat(exactCell.contains(AtlasRowLockDescriptor.of(TABLE, BYTES_SUFFIX)))
                .isFalse();
    }

    @Test
    public void exactCellDoesNotContainOtherCellDescriptors() {
        Range<LockDescriptor> exactCell = AtlasLockDescriptorRanges.exactCell(TABLE, BYTES, BYTES);

        assertThat(exactCell.contains(AtlasCellLockDescriptor.of(TABLE, BYTES, BYTES_SUFFIX)))
                .isFalse();
        assertThat(exactCell.contains(AtlasCellLockDescriptor.of(TABLE, BYTES_SUFFIX, BYTES)))
                .isFalse();
    }
}
