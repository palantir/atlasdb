/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;

@Value.Immutable
public interface SweepBatchConfig {

    /**
     * The target maximum number of (cell, timestamp) pairs to examine in a single run of SweepTaskRunner.
     */
    int maxCellTsPairsToExamine();

    /**
     * The target number of (cell, timestamp) pairs in a batch of candidate to process at once.
     */
    int candidateBatchSize();

    /**
     * The target number of total (cell, timestamp) pairs to delete in a single batch.
     * The actual number will vary.
     */
    int deleteBatchSize();

    @Value.Check
    default void check() {
        Preconditions.checkState(maxCellTsPairsToExamine() > 0, "Number of cells to examine must be greater than zero");
        Preconditions.checkState(candidateBatchSize() > 0, "Candidate batch size must be greater than zero");
        Preconditions.checkState(deleteBatchSize() > 0, "Delete batch size must be greater than zero");
    }

    default SweepBatchConfig adjust(double multiplier) {
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(adjust(maxCellTsPairsToExamine(), multiplier))
                .candidateBatchSize(adjust(candidateBatchSize(), multiplier))
                .deleteBatchSize(adjust(deleteBatchSize(), multiplier))
                .build();
    }

    default int adjust(int parameterValue, double multiplier) {
        return Math.max(1, (int) (multiplier * parameterValue));
    }
}
