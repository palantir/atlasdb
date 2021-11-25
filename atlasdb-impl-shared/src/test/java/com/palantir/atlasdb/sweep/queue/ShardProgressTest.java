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
package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepShardProgressTable;
import org.junit.Before;
import org.junit.Test;

public class ShardProgressTest {
    private static final long INITIAL_TIMESTAMP = SweepQueueUtils.INITIAL_TIMESTAMP;
    private static final long RESET_TIMESTAMP = SweepQueueUtils.RESET_TIMESTAMP;

    private ShardProgress progress;
    private KeyValueService kvs;

    private static final ShardAndStrategy CONSERVATIVE_TEN = ShardAndStrategy.conservative(10);
    private static final ShardAndStrategy THOROUGH_TEN = ShardAndStrategy.thorough(10);
    private static final ShardAndStrategy CONSERVATIVE_TWENTY = ShardAndStrategy.conservative(20);

    private static final Cell DUMMY = Cell.create(new byte[] {0}, new byte[] {0});

    @Before
    public void setup() {
        kvs = new InMemoryKeyValueService(true);
        progress = new ShardProgress(kvs);
    }

    @Test
    public void canReadInitialNumberOfShards() {
        assertThat(progress.getNumberOfShards()).isEqualTo(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
    }

    @Test
    public void canUpgradeNumberOfShardsIfPersistedDefaultValue() {
        byte[] defaultValue = ShardProgress.createColumnValue(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
        CheckAndSetRequest request = ShardProgress.createNewCellRequest(ShardProgress.SHARD_COUNT_SAS, defaultValue);
        kvs.checkAndSet(request);

        progress.updateNumberOfShards(128);
        assertThat(progress.getNumberOfShards()).isEqualTo(128);
    }

    @Test
    public void canUpdateNumberOfShards() {
        progress.updateNumberOfShards(128);
        assertThat(progress.getNumberOfShards()).isEqualTo(128);
    }

    @Test
    public void cannotUpdateNumberOfShardsToZero() {
        progress.updateNumberOfShards(0);
        assertThat(progress.getNumberOfShards()).isEqualTo(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
    }

    @Test
    public void attemptingToDecreaseNumberOfShardsIsNoop() {
        progress.updateNumberOfShards(64);
        progress.updateNumberOfShards(32);
        assertThat(progress.getNumberOfShards()).isEqualTo(64);
    }

    @Test
    public void canIncreaseNumberOfShardsToMax() {
        progress.updateNumberOfShards(AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS);
        assertThat(progress.getNumberOfShards()).isEqualTo(AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS);
    }

    @Test
    public void increasingNumberOfShardsAboveMaxThrows() {
        assertThatThrownBy(() -> progress.updateNumberOfShards(AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canReadInitialSweptTimestamp() {
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
    }

    @Test
    public void canUpdateSweptTimestamp() {
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 1024L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(1024L);
    }

    @Test
    public void attemptingToDecreaseSweptTimestampIsNoop() {
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 1024L);
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 512L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneShardDoesNotAffectOthers() {
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TWENTY)).isEqualTo(INITIAL_TIMESTAMP);
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 1024L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TWENTY)).isEqualTo(INITIAL_TIMESTAMP);

        progress.updateLastSweptTimestamp(CONSERVATIVE_TWENTY, 512L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TWENTY)).isEqualTo(512L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneConsistencyDoesNotAffectOther() {
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        assertThat(progress.getLastSweptTimestamp(THOROUGH_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 128L);
        assertThat(progress.getLastSweptTimestamp(THOROUGH_TEN)).isEqualTo(INITIAL_TIMESTAMP);

        progress.updateLastSweptTimestamp(THOROUGH_TEN, 32L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(128L);
        assertThat(progress.getLastSweptTimestamp(THOROUGH_TEN)).isEqualTo(32L);
    }

    @Test
    public void updatingTimestampsDoesNotAffectShardsAndViceVersa() {
        assertThat(progress.getNumberOfShards()).isEqualTo(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        assertThat(progress.getLastSweptTimestamp(THOROUGH_TEN)).isEqualTo(INITIAL_TIMESTAMP);

        progress.updateNumberOfShards(64);
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 32L);
        progress.updateLastSweptTimestamp(THOROUGH_TEN, 128L);

        assertThat(progress.getNumberOfShards()).isEqualTo(64);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(32L);
        assertThat(progress.getLastSweptTimestamp(THOROUGH_TEN)).isEqualTo(128L);
    }

    @Test
    public void progressivelyFailingCasEventuallySucceeds() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        when(mockKvs.get(any(), anyMap()))
                .thenReturn(ImmutableMap.of())
                .thenReturn(ImmutableMap.of(DUMMY, createValue(5L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(10L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(15L)));
        doThrow(new CheckAndSetException("sadness")).when(mockKvs).checkAndSet(any(CheckAndSetRequest.class));
        ShardProgress instrumentedProgress = new ShardProgress(mockKvs);

        assertThat(instrumentedProgress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 12L))
                .isEqualTo(15L);
    }

    @Test
    public void repeatedlyFailingCasThrows() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        when(mockKvs.get(any(), anyMap()))
                .thenReturn(ImmutableMap.of())
                .thenReturn(ImmutableMap.of(DUMMY, createValue(5L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(10L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(10L)));
        doThrow(new CheckAndSetException("sadness")).when(mockKvs).checkAndSet(any(CheckAndSetRequest.class));
        ShardProgress instrumentedProgress = new ShardProgress(mockKvs);

        assertThatThrownBy(() -> instrumentedProgress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 12L))
                .isInstanceOf(CheckAndSetException.class);
    }

    @Test
    public void canResetProgressForSpecificShards() {
        progress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 8888L);
        progress.updateLastSweptTimestamp(CONSERVATIVE_TWENTY, 8888L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(8888L);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TWENTY)).isEqualTo(8888L);

        progress.resetProgressForShard(CONSERVATIVE_TEN);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TEN)).isEqualTo(RESET_TIMESTAMP);
        assertThat(progress.getLastSweptTimestamp(CONSERVATIVE_TWENTY)).isEqualTo(8888L);
    }

    @Test
    public void stopsTryingToResetIfSomeoneElseDid() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        when(mockKvs.get(any(), anyMap()))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(8L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(4L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(RESET_TIMESTAMP)));
        doThrow(new CheckAndSetException("sadness")).when(mockKvs).checkAndSet(any(CheckAndSetRequest.class));
        ShardProgress instrumentedProgress = new ShardProgress(mockKvs);

        assertThatCode(() -> instrumentedProgress.resetProgressForShard(CONSERVATIVE_TEN))
                .doesNotThrowAnyException();
    }

    @Test
    public void repeatedlyFailingCasThrowsForReset() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        when(mockKvs.get(any(), anyMap()))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(8L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(9L)))
                .thenReturn(ImmutableMap.of(DUMMY, createValue(10L)));
        doThrow(new CheckAndSetException("sadness")).when(mockKvs).checkAndSet(any(CheckAndSetRequest.class));
        ShardProgress instrumentedProgress = new ShardProgress(mockKvs);

        assertThatCode(() -> instrumentedProgress.resetProgressForShard(CONSERVATIVE_TEN))
                .isInstanceOf(CheckAndSetException.class);
        verify(mockKvs, times(3)).checkAndSet(any(CheckAndSetRequest.class));
    }

    private Value createValue(long num) {
        SweepShardProgressTable.Value value = SweepShardProgressTable.Value.of(num);
        return Value.create(value.persistValue(), 0L);
    }
}
