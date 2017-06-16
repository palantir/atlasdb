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

package com.palantir.atlasdb.config;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;

@JsonDeserialize(as = ImmutableAtlasDbRuntimeConfig.class)
@JsonSerialize(as = ImmutableAtlasDbRuntimeConfig.class)
@Value.Immutable
public abstract class AtlasDbRuntimeConfig {
    /**
     * If true, a background thread will periodically delete cells that
     * have been overwritten or deleted. This differs from scrubbing
     * because it is an untargeted cleaning process that scans all data
     * looking for cells to delete.
     */
    // TODO(ssouza): make this Default after removing support for mirror method on the AtlasDbConfig.
    @Nullable
    public abstract Boolean enableSweep();

    /**
     * The number of milliseconds to wait between each batch of cells
     * processed by the background sweeper.
     */
    // TODO(ssouza): make this Default after removing support for mirror method on the AtlasDbConfig. We need to detect
    // the absence of this value for now.
    @Nullable
    public abstract Long getSweepPauseMillis();

    /**
     * The target number of (cell, timestamp) pairs to examine in a single run of the background sweeper.
     */
    // TODO(ssouza): make this Default after removing support for mirror method on the AtlasDbConfig. We need to detect
    // the absence of this value for now.
    @Nullable
    public abstract Integer getSweepReadLimit();

    /**
     * The target number of candidate (cell, timestamp) pairs to load per batch while sweeping.
     */
    // TODO(ssouza): make this Default after removing support for mirror method on the AtlasDbConfig. We need to detect
    // the absence of this value for now.
    @Nullable
    public abstract Integer getSweepCandidateBatchHint();

    /**
     * The target number of (cell, timestamp) pairs to delete at once while sweeping.
     */
    // TODO(ssouza): make this Default after removing support for mirror method on the AtlasDbConfig. We need to detect
    // the absence of this value for now.
    @Nullable
    public abstract Integer getSweepDeleteBatchHint();

    public static ImmutableAtlasDbRuntimeConfig create(AtlasDbConfig config) {
        return ImmutableAtlasDbRuntimeConfig.builder()
                .enableSweep(config.enableSweep())
                .sweepPauseMillis(config.getSweepPauseMillis())
                .sweepCandidateBatchHint(config.getSweepCandidateBatchHint())
                .sweepReadLimit(config.getSweepReadLimit())
                .sweepDeleteBatchHint(config.getSweepDeleteBatchHint())
                .build();
    }
}
