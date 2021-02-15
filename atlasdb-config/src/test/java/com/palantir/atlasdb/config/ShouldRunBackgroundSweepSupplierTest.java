/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import java.util.function.Supplier;
import org.junit.Test;

public class ShouldRunBackgroundSweepSupplierTest {
    private static final TargetedSweepInstallConfig SWEEP_QUEUE_WRITES_ENABLED =
            ImmutableTargetedSweepInstallConfig.builder()
                    .enableSweepQueueWrites(true)
                    .build();
    private static final TargetedSweepInstallConfig SWEEP_QUEUE_WRITES_DISABLED =
            ImmutableTargetedSweepInstallConfig.builder()
                    .enableSweepQueueWrites(false)
                    .build();

    private static final TargetedSweepRuntimeConfig TARGETED_SWEEP_ENABLED =
            ImmutableTargetedSweepRuntimeConfig.builder().enabled(true).build();
    private static final TargetedSweepRuntimeConfig TARGETED_SWEEP_DISABLED =
            ImmutableTargetedSweepRuntimeConfig.builder().enabled(false).build();

    private static final SweepConfig BACKGROUND_SWEEP_ENABLED =
            ImmutableSweepConfig.builder().enabled(true).build();
    private static final SweepConfig BACKGROUND_SWEEP_UNSET = SweepConfig.defaultSweepConfig();
    private static final SweepConfig BACKGROUND_SWEEP_DISABLED = SweepConfig.disabled();

    @Test
    public void disableBackgroundSweepIfBackgroundSweepExplicitlyDisabled() {
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_DISABLED, TARGETED_SWEEP_DISABLED, BACKGROUND_SWEEP_DISABLED))
                .isFalse();
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_ENABLED, TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_DISABLED))
                .isFalse();
    }

    @Test
    public void enableBackgroundSweepIfBackgroundSweepExplicitlyEnabled() {
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_DISABLED, TARGETED_SWEEP_DISABLED, BACKGROUND_SWEEP_ENABLED))
                .isTrue();
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_ENABLED, TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_ENABLED))
                .isTrue();
    }

    @Test
    public void disableBackgroundSweepIfNotSetAndTargetedSweepQueueWritesEnabled() {
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_ENABLED, TARGETED_SWEEP_DISABLED, BACKGROUND_SWEEP_UNSET))
                .isFalse();
    }

    @Test
    public void enableBackgroundSweepIfNotSetAndTargetedSweepQueueWritesDisabled() {
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_DISABLED, TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_UNSET))
                .isTrue();
    }

    @Test
    public void disableBackgroundSweepIfNotSetAndTargetedSweepFullyEnabled() {
        assertThat(runBackgroundSweep(SWEEP_QUEUE_WRITES_ENABLED, TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_UNSET))
                .isFalse();
    }

    @SuppressWarnings("unchecked") // Mock assignment known to be safe
    @Test
    public void liveReload() {
        Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier = mock(Supplier.class);
        when(runtimeConfigSupplier.get())
                .thenReturn(createRuntimeConfig(TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_UNSET))
                .thenReturn(createRuntimeConfig(TARGETED_SWEEP_DISABLED, BACKGROUND_SWEEP_UNSET))
                .thenReturn(createRuntimeConfig(TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_ENABLED));

        ShouldRunBackgroundSweepSupplier supplier = new ShouldRunBackgroundSweepSupplier(
                () -> runtimeConfigSupplier.get().sweep(), SWEEP_QUEUE_WRITES_ENABLED.enableSweepQueueWrites());

        assertThat(supplier.getAsBoolean())
                .as("TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_UNSET")
                .isFalse();
        assertThat(supplier.getAsBoolean())
                .as("TARGETED_SWEEP_DISABLED, BACKGROUND_SWEEP_UNSET")
                .isFalse();
        assertThat(supplier.getAsBoolean())
                .as("TARGETED_SWEEP_ENABLED, BACKGROUND_SWEEP_ENABLED")
                .isTrue();
    }

    private static boolean runBackgroundSweep(
            TargetedSweepInstallConfig tsInstall, TargetedSweepRuntimeConfig tsRuntime, SweepConfig bgSweepConfig) {
        return new ShouldRunBackgroundSweepSupplier(
                        () -> createRuntimeConfig(tsRuntime, bgSweepConfig).sweep(), tsInstall.enableSweepQueueWrites())
                .getAsBoolean();
    }

    private static AtlasDbRuntimeConfig createRuntimeConfig(
            TargetedSweepRuntimeConfig targetedSweepRuntimeConfig, SweepConfig backgroundSweepConfig) {
        return ImmutableAtlasDbRuntimeConfig.builder()
                .targetedSweep(targetedSweepRuntimeConfig)
                .sweep(backgroundSweepConfig)
                .build();
    }
}
