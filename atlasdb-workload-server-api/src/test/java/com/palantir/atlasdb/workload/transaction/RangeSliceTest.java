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
import org.junit.Test;

public class RangeSliceTest {
    @Test
    public void cannotCreateRangeWithStartColumnGreaterThanEndColumn() {
        assertThatLoggableExceptionThrownBy(() ->
                        RangeSlice.builder().startInclusive(5).endExclusive(4).build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Start must be less than or equal to end")
                .hasExactlyArgs(
                        SafeArg.of("startInclusive", Optional.of(5)), SafeArg.of("endExclusive", Optional.of(4)));
    }

    @Test
    public void emptyRangeDoesNotContainAnyElements() {
        RangeSlice emptyRange =
                RangeSlice.builder().startInclusive(6).endExclusive(6).build();
        assertThat(emptyRange.contains(5)).isFalse();
        assertThat(emptyRange.contains(6)).isFalse();
        assertThat(emptyRange.contains(7)).isFalse();
        assertThat(emptyRange.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(emptyRange.contains(Integer.MAX_VALUE)).isFalse();
    }

    @Test
    public void singleElementRangeContainsJustItself() {
        RangeSlice justSeven =
                RangeSlice.builder().startInclusive(7).endExclusive(8).build();
        assertThat(justSeven.contains(6)).isFalse();
        assertThat(justSeven.contains(7)).isTrue();
        assertThat(justSeven.contains(8)).isFalse();
        assertThat(justSeven.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(justSeven.contains(Integer.MAX_VALUE)).isFalse();
    }

    @Test
    public void universalRangeContainsEverything() {
        RangeSlice universal = RangeSlice.builder().build();
        assertThat(universal.contains(10241024)).isTrue();
        assertThat(universal.contains(-20482048)).isTrue();
        assertThat(universal.contains(40924092)).isTrue();
        assertThat(universal.contains(Integer.MIN_VALUE)).isTrue();
        assertThat(universal.contains(Integer.MAX_VALUE)).isTrue();
    }

    @Test
    public void lowerBoundedRangesContainCorrectElements() {
        RangeSlice fiveOrGreater = RangeSlice.builder().startInclusive(5).build();
        assertThat(fiveOrGreater.contains(4)).isFalse();
        assertThat(fiveOrGreater.contains(5)).isTrue();
        assertThat(fiveOrGreater.contains(6)).isTrue();
        assertThat(fiveOrGreater.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(fiveOrGreater.contains(Integer.MAX_VALUE)).isTrue();
    }

    @Test
    public void upperBoundedRangesContainCorrectElements() {
        RangeSlice lessThanFive = RangeSlice.builder().endExclusive(5).build();
        assertThat(lessThanFive.contains(4)).isTrue();
        assertThat(lessThanFive.contains(5)).isFalse();
        assertThat(lessThanFive.contains(6)).isFalse();
        assertThat(lessThanFive.contains(Integer.MIN_VALUE)).isTrue();
        assertThat(lessThanFive.contains(Integer.MAX_VALUE)).isFalse();
    }
}
