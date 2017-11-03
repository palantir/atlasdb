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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Created by tboam on 03/11/2017.
 */
public class AdjustableSweepBatchConfigSourceTest {

    private AdjustableSweepBatchConfigSource adjustableConfig;
    private double previousMultiplier;
    private int previousMaxCellTsPairsToExamine;
    private int previousCandidateBatchSize;
    private int previousDeleteBatchSize;

    @Test
    public void batchSizeMultiplierDecreasesOnFailure() {
        // Given
        configWithValues(1000, 1000, 1000);

        // When
        adjustableConfig.decreaseMultiplier();

        // Then
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(lessThan(previousMultiplier)));
    }

    @Test
    public void canDecreaseAndIncreaseConfigWithAllSmallValues() {
        //Given
        configWithValues(1, 1, 1);

        whenDecreasingTheMultiplier_thenAdjustedConfigValuesDecrease();

        whenIncreasingTheMultiplier_thenAdjustedConfigValuesIncrease();

        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(1.0));
    }

    @Test
    public void canDecreaseAndIncreaseConfigWithAllLargeValues() {
        //Given
        configWithValues(1000, 1000, 1000);

        whenDecreasingTheMultiplier_thenAdjustedConfigValuesDecrease();

        whenIncreasingTheMultiplier_thenAdjustedConfigValuesIncrease();

        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(1.0));
    }

    @Test
    public void canDecreaseAndIncreaseConfigWithMixOfValues() {
        //Given
        configWithValues(1000, 1, 100);

        whenDecreasingTheMultiplier_thenAdjustedConfigValuesDecrease();

        whenIncreasingTheMultiplier_thenAdjustedConfigValuesIncrease();

        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(1.0));
    }

    private void whenIncreasingTheMultiplier_thenAdjustedConfigValuesIncrease() {
        for (int i = 0; i < 1_000; i++) {
            // When
            adjustableConfig.increaseMultiplier();

            // Then
            batchSizeMultiplierIncreases();
            batchSizeMultiplierDoesNotExceedOne();

            maxCellTsPairsToExamineDoesNotDecrease();
            candidateBatchSizeDoesNotDecrease();
            deleteBatchSizeDoesNotDecrease();

            maxCellTsPairsToExamineDoesNotExceedBaseConfig();
            candidateBatchSizeDoesNotExceedBaseConfig();
            deleteBatchSizeDoesNotExceedBaseConfig();

            updatePreviousValues();
        }
    }

    private void whenDecreasingTheMultiplier_thenAdjustedConfigValuesDecrease() {
        for (int i = 0; i < 10_000; i++) {
            // When
            adjustableConfig.decreaseMultiplier();

            // Then
            batchSizeMultiplierDecreases();
            maxCellTsPairsToExamineDecreasesToAMinimumOfOne();
            candidateBatchSizeDecreasesToAMinimumOfOne();
            deleteBatchSizeDecreasesToAMinimumOfOne();

            updatePreviousValues();
        }
    }

    private void configWithValues(int maxCellTsPairsToExamine, int candidateBatchSize, int deleteBatchSize) {
        adjustableConfig = AdjustableSweepBatchConfigSource.create(() -> new SweepBatchConfig() {
            @Override
            public int maxCellTsPairsToExamine() {
                return maxCellTsPairsToExamine;
            }

            @Override
            public int candidateBatchSize() {
                return candidateBatchSize;
            }

            @Override
            public int deleteBatchSize() {
                return deleteBatchSize;
            }
        });

        updatePreviousValues();
    }

    private void batchSizeMultiplierDecreases() {
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(lessThanOrEqualTo(previousMultiplier)));
    }

    private void maxCellTsPairsToExamineDecreasesToAMinimumOfOne() {
        int newValue = adjustableConfig.getAdjustedSweepConfig().maxCellTsPairsToExamine();
        if (newValue == 1) {
            return;
        }
        assertThat(newValue, is(lessThan(previousMaxCellTsPairsToExamine)));
    }

    private void candidateBatchSizeDecreasesToAMinimumOfOne() {
        int newValue = adjustableConfig.getAdjustedSweepConfig().candidateBatchSize();
        if (newValue == 1) {
            return;
        }
        assertThat(newValue, is(lessThan(previousCandidateBatchSize)));
    }

    private void deleteBatchSizeDecreasesToAMinimumOfOne() {
        int newValue = adjustableConfig.getAdjustedSweepConfig().deleteBatchSize();
        if (newValue == 1) {
            return;
        }
        assertThat(newValue, is(lessThan(previousDeleteBatchSize)));
    }

    private void batchSizeMultiplierIncreases() {
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(greaterThanOrEqualTo(previousMultiplier)));
    }

    private void batchSizeMultiplierDoesNotExceedOne() {
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(lessThanOrEqualTo(1.0)));
    }

    private void maxCellTsPairsToExamineDoesNotDecrease() {
        assertThat(adjustableConfig.getAdjustedSweepConfig().maxCellTsPairsToExamine(),
                is(greaterThanOrEqualTo(previousMaxCellTsPairsToExamine)));
    }

    private void candidateBatchSizeDoesNotDecrease() {
        assertThat(adjustableConfig.getAdjustedSweepConfig().candidateBatchSize(),
                is(greaterThanOrEqualTo(previousCandidateBatchSize)));

    }

    private void deleteBatchSizeDoesNotDecrease() {
        assertThat(adjustableConfig.getAdjustedSweepConfig().deleteBatchSize(),
                is(greaterThanOrEqualTo(previousDeleteBatchSize)));
    }

    private void maxCellTsPairsToExamineDoesNotExceedBaseConfig() {
        assertThat(adjustableConfig.getAdjustedSweepConfig().maxCellTsPairsToExamine(),
                is(lessThanOrEqualTo(adjustableConfig.getRawSweepConfig().maxCellTsPairsToExamine())));
    }

    private void candidateBatchSizeDoesNotExceedBaseConfig() {
        assertThat(adjustableConfig.getAdjustedSweepConfig().candidateBatchSize(),
                is(lessThanOrEqualTo(adjustableConfig.getRawSweepConfig().candidateBatchSize())));
    }

    private void deleteBatchSizeDoesNotExceedBaseConfig() {
        assertThat(adjustableConfig.getAdjustedSweepConfig().deleteBatchSize(),
                is(lessThanOrEqualTo(adjustableConfig.getRawSweepConfig().deleteBatchSize())));
    }

    private void updatePreviousValues() {
        previousMultiplier = adjustableConfig.getBatchSizeMultiplier();
        previousMaxCellTsPairsToExamine = adjustableConfig.getAdjustedSweepConfig().maxCellTsPairsToExamine();
        previousCandidateBatchSize = adjustableConfig.getAdjustedSweepConfig().candidateBatchSize();
        previousDeleteBatchSize = adjustableConfig.getAdjustedSweepConfig().deleteBatchSize();
    }
}
