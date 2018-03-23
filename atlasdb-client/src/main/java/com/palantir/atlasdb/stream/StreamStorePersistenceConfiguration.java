/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.stream;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;

@Value.Immutable
public interface StreamStorePersistenceConfiguration {
    StreamStorePersistenceConfiguration DEFAULT_CONFIG = ImmutableStreamStorePersistenceConfiguration.builder()
            .build();

    @Value.Default
    default int numBlocksToWriteBeforePause() {
        return 1;
    }

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
