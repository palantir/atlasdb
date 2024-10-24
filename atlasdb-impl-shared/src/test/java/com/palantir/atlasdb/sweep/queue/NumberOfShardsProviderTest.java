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
package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.NumberOfShardsProvider.MismatchBehaviour;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NumberOfShardsProviderTest {
    private final Supplier<Integer> runtimeConfigSupplier = mock(Supplier.class);

    private ShardProgress progress;
    private Supplier<Integer> numShardSupplier;

    @BeforeEach
    public void setup() {
        KeyValueService kvs = new InMemoryKeyValueService(true);
        progress = spy(new ShardProgress(kvs));
        numShardSupplier = NumberOfShardsProvider.createMemoizingProvider(
                progress, runtimeConfigSupplier, MismatchBehaviour.UPDATE, Duration.ofMillis(1))::getNumberOfShards;
    }

    @Test
    public void testDefaultValue() {
        assertThat(setRuntimeAndGetNumberOfShards(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS))
                .isEqualTo(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
    }

    @Test
    public void testConfigHigherValuePersistedInProgress() {
        assertThat(setRuntimeAndGetNumberOfShards(50)).isEqualTo(50);
        assertThat(progress.getNumberOfShards()).isEqualTo(50);
    }

    @Test
    public void testProgressHigherValue() {
        when(runtimeConfigSupplier.get()).thenReturn(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
        progress.updateNumberOfShards(25);
        assertThat(numShardSupplier.get()).isEqualTo(25);
    }

    @Test
    public void loweringConfigDoesNotReduceNumberOfShards() throws InterruptedException {
        assertThat(setRuntimeAndGetNumberOfShards(75)).isEqualTo(75);
        waitToForceRefresh();
        assertThat(setRuntimeAndGetNumberOfShards(50)).isEqualTo(75);
    }

    @Test
    public void changingRuntimeConfigHasNoEffectUntilGetIsCalled() throws InterruptedException {
        assertThat(setRuntimeAndGetNumberOfShards(50)).isEqualTo(50);
        waitToForceRefresh();

        when(runtimeConfigSupplier.get()).thenReturn(150);
        when(runtimeConfigSupplier.get()).thenReturn(100);
        assertThat(setRuntimeAndGetNumberOfShards(125)).isEqualTo(125);
    }

    @Test
    public void getAfterRefreshTimeChecksConfigAndUpdatesProgress() throws InterruptedException {
        assertThat(setRuntimeAndGetNumberOfShards(50)).isEqualTo(50);
        verify(runtimeConfigSupplier, times(1)).get();
        verify(progress, times(1)).updateNumberOfShards(50);
        waitToForceRefresh();

        assertThat(setRuntimeAndGetNumberOfShards(100)).isEqualTo(100);
        verify(runtimeConfigSupplier, times(2)).get();
        verify(progress, times(1)).updateNumberOfShards(100);
        waitToForceRefresh();

        assertThat(setRuntimeAndGetNumberOfShards(75)).isEqualTo(100);
        verify(runtimeConfigSupplier, times(3)).get();
        verify(progress, times(1)).updateNumberOfShards(75);

        verify(progress, times(3)).updateNumberOfShards(anyInt());
    }

    @Test
    public void getBeforeRefreshTimeDoesNotCheckConfigOrUpdateProgress() throws InterruptedException {
        numShardSupplier = NumberOfShardsProvider.createMemoizingProvider(
                progress, runtimeConfigSupplier, MismatchBehaviour.UPDATE, Duration.ofSeconds(100))::getNumberOfShards;
        assertThat(setRuntimeAndGetNumberOfShards(50)).isEqualTo(50);

        assertThat(setRuntimeAndGetNumberOfShards(100)).isEqualTo(50);
        progress.updateNumberOfShards(125);
        assertThat(numShardSupplier.get()).isEqualTo(50);
        verify(runtimeConfigSupplier, times(1)).get();
        verify(progress, times(1)).updateNumberOfShards(50);
        // second update was us setting it, so wasn't invoked by get()
        verify(progress, times(2)).updateNumberOfShards(anyInt());
    }

    @Test
    public void doesNotUpdateIfMismatchBetweenProgressAndConfigAndIgnoreBehaviourSet() {
        numShardSupplier = NumberOfShardsProvider.createMemoizingProvider(
                progress,
                runtimeConfigSupplier,
                MismatchBehaviour.IGNORE_UPDATES,
                Duration.ofMillis(SweepQueueUtils.REFRESH_TIME))::getNumberOfShards;
        progress.updateNumberOfShards(75);
        assertThat(setRuntimeAndGetNumberOfShards(100)).isEqualTo(75);
    }

    private int setRuntimeAndGetNumberOfShards(int runtime) {
        when(runtimeConfigSupplier.get()).thenReturn(runtime);
        return numShardSupplier.get();
    }

    private void waitToForceRefresh() throws InterruptedException {
        Thread.sleep(5);
    }
}
