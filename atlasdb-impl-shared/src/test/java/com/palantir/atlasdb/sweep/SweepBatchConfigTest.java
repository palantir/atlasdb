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
package com.palantir.atlasdb.sweep;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("CheckReturnValue")
public class SweepBatchConfigTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void canCreateConfig() {
        ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(1)
                .candidateBatchSize(1)
                .deleteBatchSize(1)
                .build();
    }

    @Test
    public void canNotCreateConfigWithZeroCellsToExamine() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("cells to examine");

        ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(0)
                .candidateBatchSize(1)
                .deleteBatchSize(1)
                .build();
    }

    @Test
    public void canNotCreateConfigWithZeroCandidateBatchSize() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Candidate batch size");

        ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(1)
                .candidateBatchSize(0)
                .deleteBatchSize(1)
                .build();
    }

    @Test
    public void canNotCreateConfigWithZeroDeleteBatchSize() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Delete batch size");

        ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(1)
                .candidateBatchSize(1)
                .deleteBatchSize(0)
                .build();
    }
}
