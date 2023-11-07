/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.transaction;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ColumnRangeSelectionTest {
    @Test
    public void cannotCreateRangeWithStartColumnGreaterThanEndColumn() {
        assertThatLoggableExceptionThrownBy(() -> ColumnRangeSelection.builder()
                        .startColumnInclusive(5)
                        .endColumnExclusive(4)
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Start column must be less than or equal to end column")
                .hasExactlyArgs(
                        SafeArg.of("startColumnInclusive", Optional.of(5)),
                        SafeArg.of("endColumnExclusive", Optional.of(4)));
    }

    @Test
    public void emptyRangeDoesNotContainAnyElements() {
        ColumnRangeSelection emptyRange = ColumnRangeSelection.builder()
                .startColumnInclusive(6)
                .endColumnExclusive(6)
                .build();
        assertThat(emptyRange.contains(5)).isFalse();
        assertThat(emptyRange.contains(6)).isFalse();
        assertThat(emptyRange.contains(7)).isFalse();
        assertThat(emptyRange.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(emptyRange.contains(Integer.MAX_VALUE)).isFalse();
    }

    @Test
    public void singleElementRangeContainsJustItself() {
        ColumnRangeSelection justSeven = ColumnRangeSelection.builder()
                .startColumnInclusive(7)
                .endColumnExclusive(8)
                .build();
        assertThat(justSeven.contains(6)).isFalse();
        assertThat(justSeven.contains(7)).isTrue();
        assertThat(justSeven.contains(8)).isFalse();
        assertThat(justSeven.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(justSeven.contains(Integer.MAX_VALUE)).isFalse();
    }

    @Test
    public void universalRangeContainsEverything() {
        ColumnRangeSelection universal = ColumnRangeSelection.builder().build();
        assertThat(universal.contains(10241024)).isTrue();
        assertThat(universal.contains(-20482048)).isTrue();
        assertThat(universal.contains(40924092)).isTrue();
        assertThat(universal.contains(Integer.MIN_VALUE)).isTrue();
        assertThat(universal.contains(Integer.MAX_VALUE)).isTrue();
    }

    @Test
    public void lowerBoundedRangesContainCorrectElements() {
        ColumnRangeSelection universal =
                ColumnRangeSelection.builder().startColumnInclusive(5).build();
        assertThat(universal.contains(4)).isFalse();
        assertThat(universal.contains(5)).isTrue();
        assertThat(universal.contains(6)).isTrue();
        assertThat(universal.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(universal.contains(Integer.MAX_VALUE)).isTrue();
    }

    @Test
    public void upperBoundedRangesContainCorrectElements() {
        ColumnRangeSelection universal =
                ColumnRangeSelection.builder().endColumnExclusive(5).build();
        assertThat(universal.contains(4)).isTrue();
        assertThat(universal.contains(5)).isFalse();
        assertThat(universal.contains(6)).isFalse();
        assertThat(universal.contains(Integer.MIN_VALUE)).isTrue();
        assertThat(universal.contains(Integer.MAX_VALUE)).isFalse();
    }
}
