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
package com.palantir.atlasdb.stream;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableStreamStorePersistenceConfiguration.class)
@JsonDeserialize(as = ImmutableStreamStorePersistenceConfiguration.class)
@Value.Immutable
public interface StreamStorePersistenceConfiguration {
    StreamStorePersistenceConfiguration DEFAULT_CONFIG = ImmutableStreamStorePersistenceConfiguration.builder()
            .build();

    /**
     * The number of blocks that a nontransactional storeStream() will store before pausing for
     * writePauseDurationMillis. For example, if a value of 5 is specified, then storeStream() will
     * write blocks 0 through 4 (inclusive) of a stream before sleeping; then 5 through 9, etc.
     *
     * This parameter is live reloadable. If live reloaded, we guarantee that after the next sleep, storeStream()
     * will store the new number of blocks to write before pausing. The number of blocks written from the time the
     * config parameter is reloaded until the next pause is guaranteed to be bounded by the sum of the old and new
     * numbers of blocks.
     */
    @Value.Default
    default long numBlocksToWriteBeforePause() {
        return 1;
    }

    /**
     * The number of milliseconds to pause nontransactional storeStream() operations for, following the
     * back-pressure mechanism described under numBlocksToWriteBeforePause.
     *
     * This parameter is live reloadable. If live reloaded, we guarantee that future pauses will sleep for the new
     * value in configuration. However, no guarantees are made if the storeStream() call in flight is currently asleep.
     */
    @Value.Default
    default long writePauseDurationMillis() {
        return 0;
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(numBlocksToWriteBeforePause() > 0,
                "Number of blocks to write before pausing must be positive");
        Preconditions.checkState(writePauseDurationMillis() >= 0,
                "Pause duration between batches of writes must be non-negative");
    }
}
