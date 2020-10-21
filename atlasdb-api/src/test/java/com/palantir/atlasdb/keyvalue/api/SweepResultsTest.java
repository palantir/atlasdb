/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import java.util.Optional;
import org.junit.Test;

public class SweepResultsTest {
    private static final SweepResults RESULTS = SweepResults.builder()
            .nextStartRow(Optional.of(PtBytes.toBytes("ABC")))
            .cellTsPairsExamined(1000L)
            .staleValuesDeleted(100L)
            .minSweptTimestamp(10L)
            .timeInMillis(1L)
            .timeSweepStarted(1_000L)
            .build();

    // workaround for broken equality comparison of Optional<byte[]> in Immutables
    private static final Optional<byte[]> B_WITH_FIXED_REFERENCE = Optional.of(PtBytes.toBytes("B"));
    private static final SweepResults OTHER_RESULTS = SweepResults.builder()
            .nextStartRow(B_WITH_FIXED_REFERENCE)
            .cellTsPairsExamined(2000L)
            .staleValuesDeleted(200L)
            .minSweptTimestamp(20L)
            .timeInMillis(2L)
            .timeSweepStarted(10_000L)
            .build();

    private static final SweepResults RESULTS_NO_START_ROW = SweepResults.builder()
            .nextStartRow(Optional.empty())
            .cellTsPairsExamined(3000L)
            .staleValuesDeleted(300L)
            .minSweptTimestamp(30L)
            .timeInMillis(3L)
            .timeSweepStarted(100_000L)
            .build();

    private static final SweepResults COMBINED_RESULTS_AND_OTHER = SweepResults.builder()
            .nextStartRow(B_WITH_FIXED_REFERENCE)
            .cellTsPairsExamined(1000L + 2000L)
            .staleValuesDeleted(100L + 200L)
            .minSweptTimestamp(10L)
            .timeInMillis(1L + 2L)
            .timeSweepStarted(1_000L)
            .build();

    @Test
    public void accumulateWithNextRowInOrder() {
        assertThat(RESULTS.accumulateWith(OTHER_RESULTS)).isEqualTo(COMBINED_RESULTS_AND_OTHER);
    }

    @Test
    public void accumulateWithNextRowInOppositeOrder() {
        assertThat(OTHER_RESULTS.accumulateWith(RESULTS)).isEqualTo(COMBINED_RESULTS_AND_OTHER);
    }

    @Test
    public void accumulateWithNoNextRowInOrder() {
        assertThat(RESULTS.accumulateWith(RESULTS_NO_START_ROW))
                .isEqualTo(SweepResults.builder()
                        .nextStartRow(Optional.empty())
                        .cellTsPairsExamined(1000L + 3000L)
                        .staleValuesDeleted(100L + 300L)
                        .minSweptTimestamp(10L)
                        .timeInMillis(1L + 3L)
                        .timeSweepStarted(1_000L)
                        .build());
    }

    @Test
    public void accumulateWithNoNextRowInOppositeOrder() {
        assertThat(RESULTS_NO_START_ROW.accumulateWith(OTHER_RESULTS))
                .isEqualTo(SweepResults.builder()
                        .nextStartRow(Optional.empty())
                        .cellTsPairsExamined(2000L + 3000L)
                        .staleValuesDeleted(200L + 300L)
                        .minSweptTimestamp(20L)
                        .timeInMillis(2L + 3L)
                        .timeSweepStarted(10_000L)
                        .build());
    }

    @Test
    public void accumulateAll() {
        assertThat(RESULTS_NO_START_ROW.accumulateWith(RESULTS).accumulateWith(OTHER_RESULTS))
                .isEqualTo(SweepResults.builder()
                        .nextStartRow(Optional.empty())
                        .cellTsPairsExamined(1000L + 2000L + 3000L)
                        .staleValuesDeleted(100L + 200L + 300L)
                        .minSweptTimestamp(10L)
                        .timeInMillis(1L + 2L + 3L)
                        .timeSweepStarted(1_000L)
                        .build());
    }

    @Test
    public void equalsIgnoresTimeSweepStarted() {
        SweepResults emptySweepResult = SweepResults.createEmptySweepResult(Optional.empty());
        SweepResults laterEmptySweepResult = SweepResults.builder()
                .from(emptySweepResult)
                .timeSweepStarted(emptySweepResult.getTimeSweepStarted() + 1)
                .build();

        assertThat(emptySweepResult).isEqualTo(laterEmptySweepResult);
    }
}
