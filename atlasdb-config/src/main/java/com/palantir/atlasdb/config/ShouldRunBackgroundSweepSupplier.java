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

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;

/**
 * Synthesises provided install and runtime configurations, and uses this to decide whether the background sweeper
 * should run. Specifically:
 *
 * <ul>
 *     <li>if the background sweeper has been explicitly enabled or disabled, use that setting;</li>
 *     <li>otherwise if Targeted Sweep is fully enabled (both writing to the sweep queue and actually sweeping), disable
 *     background sweep</li>
 *     <li>otherwise, follow {@link AtlasDbConstants#DEFAULT_ENABLE_SWEEP}.</li>
 * </ul>
 */
public class ShouldRunBackgroundSweepSupplier implements BooleanSupplier {
    private final TargetedSweepInstallConfig targetedSweepInstallConfig;
    private final Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier;

    public ShouldRunBackgroundSweepSupplier(
            TargetedSweepInstallConfig targetedSweepInstallConfig,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        this.targetedSweepInstallConfig = targetedSweepInstallConfig;
        this.runtimeConfigSupplier = runtimeConfigSupplier;
    }

    @Override
    public boolean getAsBoolean() {
        AtlasDbRuntimeConfig runtimeConfig = runtimeConfigSupplier.get();

        Optional<Boolean> backgroundSweepEnabled = runtimeConfig.sweep().enabled();
        if (backgroundSweepEnabled.isPresent()) {
            return backgroundSweepEnabled.get();
        }

        if (targetedSweepIsFullyEnabled(runtimeConfig.targetedSweep())) {
            return false;
        }

        return AtlasDbConstants.DEFAULT_ENABLE_SWEEP;
    }


    private boolean targetedSweepIsFullyEnabled(TargetedSweepRuntimeConfig targetedSweepRuntimeConfig) {
        return targetedSweepInstallConfig.enableSweepQueueWrites() && targetedSweepRuntimeConfig.enabled();
    }
}
