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

package com.palantir.atlasdb.sweep.asts.progress;

import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
public interface ShardProgressUpdate {
    /**
     * This indicates that sweep, for this shard, has processed all transactions starting at this timestamp or lower.
     */
    long progressTimestamp();

    /**
     * Fine timestamp partitions that sweep processed since the last successfully processed progress update.
     * Note that this is NOT simply a function of the previous progress timestamp and the current progress timestamp,
     * because Sweep is expected to not return fine partitions for which no values were actually written.
     */
    Set<Long> completedFinePartitions();

    static ImmutableShardProgressUpdate.Builder builder() {
        return ImmutableShardProgressUpdate.builder();
    }
}
