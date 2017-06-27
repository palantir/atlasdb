/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.api;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public abstract class SweepResults {

    @Value.Default
    public Optional<byte[]> getPreviousStartRow() {
        return Optional.empty();
    }

    @Value.Default
    public Optional<byte[]> getNextStartRow() {
        return Optional.empty();
    }

    /**
     * The approximate number of (cell, timestamp) pairs examined.
     * TODO: we should rename this method to something like getCellTsPairsExamined()
     */
    public abstract long getCellTsPairsExamined();

    /**
     * The number of (cell, timestamp) pairs deleted.
     * TODO: we should rename this method to something like getCellTsPairsDeleted() or staleValuesDeleted()
     */
    public abstract long getStaleValuesDeleted();

    public abstract long getSweptTimestamp();

    public static ImmutableSweepResults.Builder builder() {
        return ImmutableSweepResults.builder();
    }

    public static SweepResults createEmptySweepResult() {
        return builder()
                .cellTsPairsExamined(0)
                .staleValuesDeleted(0)
                .sweptTimestamp(0)
                .build();
    }

}
