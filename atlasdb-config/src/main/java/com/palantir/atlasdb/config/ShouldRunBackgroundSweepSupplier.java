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

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Synthesises provided install and runtime configurations, and uses this to decide whether the background sweeper
 * should run. Specifically:
 *
 * <ul>
 *     <li>if the background sweeper has been explicitly enabled or disabled, use that setting;</li>
 *     <li>otherwise disable background sweep</li>
 * </ul>
 */
public class ShouldRunBackgroundSweepSupplier implements BooleanSupplier {
    private final Supplier<SweepConfig> runtimeConfigSupplier;

    public ShouldRunBackgroundSweepSupplier(Supplier<SweepConfig> runtimeConfig) {
        this.runtimeConfigSupplier = runtimeConfig;
    }

    @Override
    public boolean getAsBoolean() {
        return runtimeConfigSupplier.get().enabled().orElse(false);
    }
}
