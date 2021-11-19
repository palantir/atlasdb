/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.google.protobuf.ByteString;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class AtlasLockDescriptorUtilsTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final byte[] NO_ZERO_ROW = new byte[] {1, 2, 3};
    private static final byte[] NO_ZERO_COL = new byte[] {4, 5, 6};
    private static final byte[] ROW_WITH_ZEROS = new byte[] {1, 0, 2, 0, 3};
    private static final byte[] COL_WITH_ZEROS = new byte[] {4, 0, 0, 5, 6};
    private static final byte[] START_WITH_ZERO = new byte[] {0, 1, 2, 3};
    private static final byte[] END_WITH_ZERO = new byte[] {4, 5, 6, 0};
    private static final byte[] SHORT_ROW = new byte[] {1};
    private static final byte[] SHORT_COL = new byte[] {2};
    private static final byte[] END_WITH_TWO_ZEROES = new byte[] {1, 3, 3, 7, 0, 0};

    @Test
    public void shortLockDescriptorCorrectlyDecodesCell() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), SHORT_ROW, SHORT_COL);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .containsExactly(CellReference.of(TABLE, Cell.create(SHORT_ROW, SHORT_COL)));
    }

    @Test
    public void lockDescriptorEndingWithTwoZeroesDecodesCorrectly() {
        LockDescriptor descriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), END_WITH_TWO_ZEROES);

        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .containsExactly(CellReference.of(TABLE, Cell.create(new byte[] {1, 3, 3, 7}, new byte[] {0})));
    }

    @Test
    public void lockDescriptorWithNoZerosReturnsEmptyForCells() {
        LockDescriptor descriptor = StringLockDescriptor.of("test");
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor)).isEmpty();
    }

    @Test
    public void lockDescriptorWithOneZeroReturnsEmptyForCells() {
        LockDescriptor descriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), NO_ZERO_ROW);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor)).isEmpty();
    }

    @Test
    public void uniqueCellLockDescriptorIsDecodedCorrectlyForCells() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), NO_ZERO_ROW, NO_ZERO_COL);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .containsExactly(CellReference.of(TABLE, Cell.create(NO_ZERO_ROW, NO_ZERO_COL)));
    }

    @Test
    public void rowWithNullsParsesAllCombinationsForCells() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), ROW_WITH_ZEROS, NO_ZERO_COL);

        List<CellReference> expected = createExpectedCells(
                Cell.create(new byte[] {1}, new byte[] {2, 0, 3, 0, 4, 5, 6}),
                Cell.create(new byte[] {1, 0, 2}, new byte[] {3, 0, 4, 5, 6}),
                Cell.create(new byte[] {1, 0, 2, 0, 3}, new byte[] {4, 5, 6}));

        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor)).isEqualTo(expected);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .contains(CellReference.of(TABLE, Cell.create(ROW_WITH_ZEROS, NO_ZERO_COL)));
    }

    @Test
    public void colWithNullsParsesAllCombinationsForCells() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), NO_ZERO_ROW, COL_WITH_ZEROS);

        List<CellReference> expected = createExpectedCells(
                Cell.create(new byte[] {1, 2, 3}, new byte[] {4, 0, 0, 5, 6}),
                Cell.create(new byte[] {1, 2, 3, 0, 4}, new byte[] {0, 5, 6}),
                Cell.create(new byte[] {1, 2, 3, 0, 4, 0}, new byte[] {5, 6}));

        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor)).isEqualTo(expected);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .contains(CellReference.of(TABLE, Cell.create(NO_ZERO_ROW, COL_WITH_ZEROS)));
    }

    @Test
    public void rowEndingWithZeroIsCorrectlyParsedForCombinationsForCells() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), END_WITH_ZERO, NO_ZERO_COL);

        List<CellReference> expected = createExpectedCells(
                Cell.create(new byte[] {4, 5, 6}, new byte[] {0, 4, 5, 6}),
                Cell.create(new byte[] {4, 5, 6, 0}, new byte[] {4, 5, 6}));

        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor)).isEqualTo(expected);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .contains(CellReference.of(TABLE, Cell.create(END_WITH_ZERO, NO_ZERO_COL)));
    }

    @Test
    public void rowStartingWithZeroIsIgnoredForCells() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), START_WITH_ZERO, NO_ZERO_COL);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .containsExactly(CellReference.of(TABLE, Cell.create(START_WITH_ZERO, NO_ZERO_COL)));
    }

    @Test
    public void columnEndingWithZeroIsIgnoredForCells() {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), NO_ZERO_ROW, END_WITH_ZERO);
        assertThat(AtlasLockDescriptorUtils.candidateCells(descriptor))
                .containsExactly(CellReference.of(TABLE, Cell.create(NO_ZERO_ROW, END_WITH_ZERO)));
    }

    @Test
    public void lockDescriptorWithOneZeroReturnsCorrectTableAndRemainder() {
        LockDescriptor descriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), NO_ZERO_ROW);
        assertThat(AtlasLockDescriptorUtils.tryParseTableRef(descriptor))
                .hasValue(ImmutableTableRefAndRemainder.of(TABLE, ByteString.copyFrom(NO_ZERO_ROW)));
    }

    @Test
    public void lockDescriptorWithMultipleZerosReturnsCorrectTableAndRemainder() {
        LockDescriptor descriptor = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW_WITH_ZEROS);
        assertThat(AtlasLockDescriptorUtils.tryParseTableRef(descriptor))
                .hasValue(ImmutableTableRefAndRemainder.of(TABLE, ByteString.copyFrom(ROW_WITH_ZEROS)));
    }

    private static List<CellReference> createExpectedCells(Cell... cells) {
        return Stream.of(cells).map(cell -> CellReference.of(TABLE, cell)).collect(Collectors.toList());
    }
}
