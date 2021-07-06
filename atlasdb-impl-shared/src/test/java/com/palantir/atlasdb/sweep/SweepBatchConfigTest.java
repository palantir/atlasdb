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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class SweepBatchConfigTest {

    @Test
    public void canCreateConfig() {
        assertThatCode(() -> ImmutableSweepBatchConfig.builder()
                        .maxCellTsPairsToExamine(1)
                        .candidateBatchSize(1)
                        .deleteBatchSize(1)
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void canNotCreateConfigWithZeroCellsToExamine() {
        assertThatThrownBy(() -> ImmutableSweepBatchConfig.builder()
                        .maxCellTsPairsToExamine(0)
                        .candidateBatchSize(1)
                        .deleteBatchSize(1)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cells to examine");
    }

    @Test
    public void canNotCreateConfigWithZeroCandidateBatchSize() {
        assertThatThrownBy(() -> ImmutableSweepBatchConfig.builder()
                        .maxCellTsPairsToExamine(1)
                        .candidateBatchSize(0)
                        .deleteBatchSize(1)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Candidate batch size");
    }

    @Test
    public void canNotCreateConfigWithZeroDeleteBatchSize() {
        assertThatThrownBy(() -> ImmutableSweepBatchConfig.builder()
                        .maxCellTsPairsToExamine(1)
                        .candidateBatchSize(1)
                        .deleteBatchSize(0)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Delete batch size");
    }
}
