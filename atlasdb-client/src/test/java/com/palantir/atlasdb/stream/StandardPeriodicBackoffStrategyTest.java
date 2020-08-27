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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class StandardPeriodicBackoffStrategyTest {
    private final StandardPeriodicBackoffStrategy.BackoffMechanism backoffMechanism
            = mock(StandardPeriodicBackoffStrategy.BackoffMechanism.class);

    @Test
    public void doesNotInvokeBackoffIfCalledWithZeroTime() {
        StreamStoreBackoffStrategy backoffStrategy = createStaticBackoffStrategy(1, 0);
        backoffStrategy.accept(1);
        verifyNoMoreInteractions(backoffMechanism);
    }

    @Test
    public void invokesBackoffIfCalledWithNonZeroTime() throws InterruptedException {
        StreamStoreBackoffStrategy backoffStrategy = createStaticBackoffStrategy(1, 100);
        backoffStrategy.accept(1);
        verify(backoffMechanism).backoff(100L);
    }

    @Test
    public void invokesBackoffMultipleTimesIfCalledWithNonZeroTime() throws InterruptedException {
        StreamStoreBackoffStrategy backoffStrategy = createStaticBackoffStrategy(1, 100);
        backoffStrategy.accept(1);
        backoffStrategy.accept(2);
        backoffStrategy.accept(3);
        backoffStrategy.accept(4);
        verify(backoffMechanism, times(4)).backoff(100L);
    }

    @Test
    public void invokesBackoffOnlyAfterWritingMultipleOfBlocksToWriteBeforePause() throws InterruptedException {
        StreamStoreBackoffStrategy backoffStrategy = createStaticBackoffStrategy(3, 100);
        backoffStrategy.accept(1);
        backoffStrategy.accept(2);
        verify(backoffMechanism, never()).backoff(anyLong());
        backoffStrategy.accept(3);
        verify(backoffMechanism).backoff(100L);

    }

    @Test
    public void invokesBackoffBasedOnFreshConfigValues() throws InterruptedException {
        StreamStorePersistenceConfiguration config1 = getStreamStorePersistenceConfiguration(1, 100);
        StreamStorePersistenceConfiguration config2 = getStreamStorePersistenceConfiguration(2, 200);
        AtomicReference<StreamStorePersistenceConfiguration> activeConfig = new AtomicReference<>(config1);

        StreamStoreBackoffStrategy backoffStrategy
                = new StandardPeriodicBackoffStrategy(activeConfig::get, backoffMechanism);

        backoffStrategy.accept(1);
        verify(backoffMechanism).backoff(100L);
        backoffStrategy.accept(2);
        verify(backoffMechanism, times(2)).backoff(100L);

        activeConfig.set(config2);
        backoffStrategy.accept(3);
        verify(backoffMechanism, never()).backoff(200L);
        backoffStrategy.accept(4);
        verify(backoffMechanism).backoff(200L);
        verifyNoMoreInteractions(backoffMechanism);

    }

    private StreamStoreBackoffStrategy createStaticBackoffStrategy(
            long blocksToWriteBeforePause, long pauseDurationMillis) {
        return new StandardPeriodicBackoffStrategy(
                () -> getStreamStorePersistenceConfiguration(blocksToWriteBeforePause, pauseDurationMillis),
                backoffMechanism);
    }

    private static StreamStorePersistenceConfiguration getStreamStorePersistenceConfiguration(
            long blocksToWriteBeforePause, long pauseDurationMillis) {
        return ImmutableStreamStorePersistenceConfiguration.builder()
                .numBlocksToWriteBeforePause(blocksToWriteBeforePause)
                .writePauseDurationMillis(pauseDurationMillis)
                .build();
    }
}
