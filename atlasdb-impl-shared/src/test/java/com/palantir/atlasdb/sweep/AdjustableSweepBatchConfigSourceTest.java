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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.function.Function;

import org.junit.Test;

/**
 * Created by tboam on 03/11/2017.
 */
public class AdjustableSweepBatchConfigSourceTest {

    private AdjustableSweepBatchConfigSource adjustableConfig;
    private SweepBatchConfig previousConfig;
    private double previousMultiplier;

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

    private void whenIncreasingTheMultiplier_thenAdjustedConfigValuesIncrease() {
        for (int i = 0; i < 1_000; i++) {
            // When
            adjustableConfig.increaseMultiplier();

            // Then
            batchSizeMultiplierIncreases();
            batchSizeMultiplierDoesNotExceedOne();

            maxCellTsPairsToExamineIncreasesBackUpToBaseConfig();
            candidateBatchSizeIncreasesBackUpToBaseConfig();
            deleteBatchSizeIncreasesBackUpToBaseConfig();

            updatePreviousValues();
        }
    }

    private void configWithValues(int maxCellTsPairsToExamine, int candidateBatchSize, int deleteBatchSize) {
        adjustableConfig = AdjustableSweepBatchConfigSource.create(() ->
                ImmutableSweepBatchConfig.builder()
                        .maxCellTsPairsToExamine(maxCellTsPairsToExamine)
                        .candidateBatchSize(candidateBatchSize)
                        .deleteBatchSize(deleteBatchSize)
                        .build()
        );

        updatePreviousValues();
    }

    private void batchSizeMultiplierDecreases() {
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(lessThanOrEqualTo(previousMultiplier)));
    }

    private void decreasesToOne(Function<SweepBatchConfig, Integer> getValue) {
        int newValue = getValue.apply(adjustableConfig.getAdjustedSweepConfig());
        int previousValue = getValue.apply(previousConfig);

        assertThat(newValue, is(anyOf(equalTo(1), lessThan(previousValue))));
    }

    private void maxCellTsPairsToExamineDecreasesToAMinimumOfOne() {
        decreasesToOne(SweepBatchConfig::maxCellTsPairsToExamine);
    }

    private void candidateBatchSizeDecreasesToAMinimumOfOne() {
        decreasesToOne(SweepBatchConfig::candidateBatchSize);
    }

    private void deleteBatchSizeDecreasesToAMinimumOfOne() {
        decreasesToOne(SweepBatchConfig::deleteBatchSize);
    }

    private void batchSizeMultiplierIncreases() {
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(greaterThanOrEqualTo(previousMultiplier)));
    }

    private void batchSizeMultiplierDoesNotExceedOne() {
        assertThat(adjustableConfig.getBatchSizeMultiplier(), is(lessThanOrEqualTo(1.0)));
    }

    private void maxCellTsPairsToExamineIncreasesBackUpToBaseConfig() {
        increasesBackUpToBaseConfig(SweepBatchConfig::maxCellTsPairsToExamine);
    }

    private void candidateBatchSizeIncreasesBackUpToBaseConfig() {
        increasesBackUpToBaseConfig(SweepBatchConfig::candidateBatchSize);
    }

    private void deleteBatchSizeIncreasesBackUpToBaseConfig() {
        increasesBackUpToBaseConfig(SweepBatchConfig::deleteBatchSize);
    }

    private void increasesBackUpToBaseConfig(Function<SweepBatchConfig, Integer> getValue) {
        assertThat(getValue.apply(adjustableConfig.getAdjustedSweepConfig()),
                is(anyOf(
                        greaterThan(getValue.apply(previousConfig)),
                        lessThanOrEqualTo(getValue.apply(adjustableConfig.getRawSweepConfig()))
                )));
    }

    private void updatePreviousValues() {
        previousMultiplier = adjustableConfig.getBatchSizeMultiplier();
        previousConfig = adjustableConfig.getAdjustedSweepConfig();
    }
}
