/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

public class OldestTargetedSweepTrackedTimestampTest {
    private final KeyValueService kvs = spy(new InMemoryKeyValueService(true));
    private final LongSupplier timestampSupplier = mock(LongSupplier.class);
    private final AtomicBoolean schedulerShutDown = new AtomicBoolean(false);
    private final DeterministicScheduler deterministicScheduler = new DeterministicScheduler() {
        @Override
        public List<Runnable> shutdownNow() {
            schedulerShutDown.set(true);
            return ImmutableList.of();
        }
    };
    private final OldestTargetedSweepTrackedTimestamp tracker =
            new OldestTargetedSweepTrackedTimestamp(kvs, timestampSupplier, deterministicScheduler);

    @Test
    public void persistsValidTimestampAndShutsDown() {
        when(timestampSupplier.getAsLong()).thenReturn(42L);

        tracker.start();
        deterministicScheduler.tick(0, TimeUnit.SECONDS);

        assertThat(tracker.getOldestTimestamp()).hasValue(42L);
        assertThat(schedulerShutDown).isTrue();

        clearInvocations(kvs);
        deterministicScheduler.tick(5, TimeUnit.SECONDS);
        verifyNoInteractions(kvs);
    }

    @Test
    public void resilientToTimestampThrowing() {
        when(timestampSupplier.getAsLong()).thenThrow(new RuntimeException()).thenReturn(17L);

        tracker.start();
        deterministicScheduler.tick(0, TimeUnit.SECONDS);

        assertThat(tracker.getOldestTimestamp()).isEmpty();

        deterministicScheduler.tick(1, TimeUnit.SECONDS);
        assertThat(tracker.getOldestTimestamp()).hasValue(17L);
        assertThat(schedulerShutDown).isTrue();
    }

    @Test
    public void doesNotIncreaseExistingTimestamp() {
        OldestTargetedSweepTrackedTimestamp otherTracker =
                new OldestTargetedSweepTrackedTimestamp(kvs, () -> 1L, deterministicScheduler);

        otherTracker.start();
        deterministicScheduler.tick(0, TimeUnit.SECONDS);

        assertThat(tracker.getOldestTimestamp()).hasValue(1L);

        when(timestampSupplier.getAsLong()).thenReturn(100L);
        tracker.start();
        deterministicScheduler.tick(1, TimeUnit.SECONDS);

        assertThat(tracker.getOldestTimestamp()).hasValue(1L);
    }

    @Test
    public void doesReduceExistingTimestamp() {
        OldestTargetedSweepTrackedTimestamp otherTracker =
                new OldestTargetedSweepTrackedTimestamp(kvs, () -> 100L, deterministicScheduler);

        otherTracker.start();
        deterministicScheduler.tick(0, TimeUnit.SECONDS);

        assertThat(tracker.getOldestTimestamp()).hasValue(100L);

        when(timestampSupplier.getAsLong()).thenReturn(13L);
        tracker.start();
        deterministicScheduler.tick(1, TimeUnit.SECONDS);

        assertThat(tracker.getOldestTimestamp()).hasValue(13L);
    }

    @Test
    public void resilientToKvsFailures() {
        KeyValueService nonInitialisedKvs = new InMemoryKeyValueService(false);
        OldestTargetedSweepTrackedTimestamp otherTracker =
                new OldestTargetedSweepTrackedTimestamp(nonInitialisedKvs, () -> 1L, deterministicScheduler);

        otherTracker.start();
        deterministicScheduler.tick(1, TimeUnit.SECONDS);

        assertThatThrownBy(otherTracker::getOldestTimestamp).isInstanceOf(RuntimeException.class);

        nonInitialisedKvs.createTable(
                ShardProgress.TABLE_REF, TableMetadata.allDefault().persistToBytes());
        deterministicScheduler.tick(1, TimeUnit.SECONDS);
        assertThat(otherTracker.getOldestTimestamp()).hasValue(1L);
    }
}
