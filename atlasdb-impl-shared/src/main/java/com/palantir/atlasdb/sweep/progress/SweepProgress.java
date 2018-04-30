/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep.progress;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;

@JsonSerialize(as = ImmutableSweepProgress.class)
@JsonDeserialize(as = ImmutableSweepProgress.class)
@Value.Immutable
public interface SweepProgress {

    TableReference tableRef();

    byte[] startRow();

    // This is currently not used, but it is included for potential future sweep optimizations
    byte[] startColumn();

    long staleValuesDeleted();

    long cellTsPairsExamined();

    long minimumSweptTimestamp();

    long timeInMillis();

    long startTimeInMillis();

    @JsonIgnore
    default SweepResults getPreviousResults() {
        return SweepResults.builder()
                .staleValuesDeleted(staleValuesDeleted())
                .cellTsPairsExamined(cellTsPairsExamined())
                .minSweptTimestamp(minimumSweptTimestamp())
                .nextStartRow(Optional.of(startRow()))
                .timeInMillis(timeInMillis())
                .timeSweepStarted(startTimeInMillis())
                .build();
    }
}
