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

import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

public class StandardPeriodicBackoffStrategy implements StreamStoreBackoffStrategy {
    private final Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration;
    private final BackoffMechanism backoff;

    @VisibleForTesting
    StandardPeriodicBackoffStrategy(Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration,
            BackoffMechanism backoff) {
        this.persistenceConfiguration = persistenceConfiguration;
        this.backoff = backoff;
    }

    public static StandardPeriodicBackoffStrategy create(
            Supplier<StreamStorePersistenceConfiguration> persistenceConfiguration) {
        return new StandardPeriodicBackoffStrategy(persistenceConfiguration, Thread::sleep);
    }

    @Override
    public void accept(long blockNumber) {
        if (persistenceConfiguration.get().writePauseDurationMillis() == 0) {
            return;
        }

        if (shouldBackoffBeforeWritingBlockNumber(blockNumber)) {
            try {
                backoff.backoff(persistenceConfiguration.get().writePauseDurationMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Preserving uninterruptibility, which is current behaviour.
            }
        }
    }

    private boolean shouldBackoffBeforeWritingBlockNumber(long blockNumber) {
        return blockNumber % persistenceConfiguration.get().numBlocksToWriteBeforePause() == 0;
    }

    interface BackoffMechanism {
        void backoff(long backoffAmount) throws InterruptedException;
    }
}
