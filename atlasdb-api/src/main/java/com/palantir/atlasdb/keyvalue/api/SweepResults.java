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

import org.immutables.value.Value;

import com.google.common.base.Optional;

@Value.Immutable
public abstract class SweepResults {

    public abstract Optional<byte[]> getPreviousStartRow();

    public abstract Optional<byte[]> getNextStartRow();

    /**
     * The approximate number of (cell, timestamp) pairs examined.
     * TODO: we should rename this method to something like getCellTsPairsExamined()
     */
    public abstract long getCellsExamined();

    /**
     * The number of (cell, timestamp) pairs deleted.
     * TODO: we should rename this method to something like getCellTsPairsDeleted()
     */
    public abstract long getCellsDeleted();

    public abstract long getSweptTimestamp();

    public static ImmutableSweepResults.Builder builder() {
        return ImmutableSweepResults.builder();
    }

    public static SweepResults createEmptySweepResult() {
        return builder()
                .cellsExamined(0)
                .cellsDeleted(0)
                .sweptTimestamp(0)
                .build();
    }

}
