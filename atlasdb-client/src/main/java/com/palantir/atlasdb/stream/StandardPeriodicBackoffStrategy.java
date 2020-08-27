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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardPeriodicBackoffStrategy implements StreamStoreBackoffStrategy {
    private final Logger log = LoggerFactory.getLogger(StandardPeriodicBackoffStrategy.class);

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
                long writePauseDurationMillis = persistenceConfiguration.get().writePauseDurationMillis();
                log.info("Invoking backoff for {} ms, because we are writing block {} of a stream",
                        SafeArg.of("blockNumber", blockNumber),
                        SafeArg.of("writePauseDurationMillis", writePauseDurationMillis));
                backoff.backoff(writePauseDurationMillis);
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
