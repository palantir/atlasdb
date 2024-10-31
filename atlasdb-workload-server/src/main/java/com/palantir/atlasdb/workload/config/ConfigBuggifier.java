/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.config;

import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.logsafe.DoNotLog;
import com.palantir.refreshable.Refreshable;
import java.security.SecureRandom;
import java.util.Optional;
import org.immutables.value.Value;

public final class ConfigBuggifier {
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private ConfigBuggifier() {
        // Utility class
    }

    // Right now we're sticking with a static runtime config. In the future, we can have a scheduled task that randomly
    // mutates the runtime config within bounds.
    public static Configs buggifyConfigs(AtlasDbConfig install, Refreshable<Optional<AtlasDbRuntimeConfig>> runtime) {
        double random = SECURE_RANDOM.nextDouble();
        if (random < 0.8) {
            AtlasDbConfig newInstallConfig = ImmutableAtlasDbConfig.copyOf(install)
                    .withTargetedSweep(ImmutableTargetedSweepInstallConfig.copyOf(install.targetedSweep())
                            .withEnableBucketBasedSweep(true));

            Refreshable<Optional<AtlasDbRuntimeConfig>> newRuntimeConfig =
                    runtime.map(maybeRuntimeConfig -> maybeRuntimeConfig.map(runtimeConfig -> {
                        TargetedSweepRuntimeConfig targetedSweepRuntimeConfig =
                                ImmutableTargetedSweepRuntimeConfig.copyOf(runtimeConfig.targetedSweep())
                                        .withShards(SECURE_RANDOM.nextInt(256) + 1); // [1, 256]
                        return ImmutableAtlasDbRuntimeConfig.copyOf(runtimeConfig)
                                .withTargetedSweep(targetedSweepRuntimeConfig);
                    }));
            return ImmutableConfigs.builder()
                    .install(newInstallConfig)
                    .runtime(newRuntimeConfig)
                    .build();
        } else {
            return ImmutableConfigs.builder().install(install).runtime(runtime).build();
        }
    }

    @Value.Immutable
    @DoNotLog
    public interface Configs {
        AtlasDbConfig install();

        Refreshable<Optional<AtlasDbRuntimeConfig>> runtime();
    }
}
