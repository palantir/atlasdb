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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.AtlasDbConstants.DEFAULT_SWEEP_QUEUE_SHARDS;
import static com.palantir.atlasdb.AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.INITIAL_TIMESTAMP;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepShardProgressTable;

public class ShardProgressTest {
    private KeyValueService kvs;
    private ShardProgress progress;

    private static final ShardAndStrategy CONSERVATIVE_TEN = ShardAndStrategy.conservative(10);
    private static final ShardAndStrategy THOROUGH_TEN = ShardAndStrategy.thorough(10);
    private static final ShardAndStrategy CONSERVATIVE_TWENTY = ShardAndStrategy.conservative(20);

    private static final Cell DUMMY = Cell.create(new byte[]{0}, new byte[]{0});

    @Before
    public void setup() {
        kvs = new InMemoryKeyValueService(true);
        progress = new ShardProgress(kvs);
    }

    @Test
    public void canReadInitialNumberOfShards() {
        assertThat(progress.getNumberOfShards()).isEqualTo(DEFAULT_SWEEP_QUEUE_SHARDS);
    }

    @Test
    public void canUpdateNumberOfShards() {
        progress.updateNumberOfShards(128);
        assertThat(progress.getNumberOfShards()).isEqualTo(128);
    }

    @Test
    public void attemptingToDecreaseNumberOfShardsIsNoop() {
        progress.updateNumberOfShards(64);
        progress.updateNumberOfShards(32);
        assertThat(progress.getNumberOfShards()).isEqualTo(64);
    }

    @Test
    public void canIncreaseNumberOfShardsToMax() {
        progress.updateNumberOfShards(MAX_SWEEP_QUEUE_SHARDS);
        assertThat(progress.getNumberOfShards()).isEqualTo(MAX_SWEEP_QUEUE_SHARDS);
    }

    @Test
    public void increasingNumberOfShardsAboveMaxThrows() {
        assertThatThrownBy(() -> progress.updateNumberOfShards(MAX_SWEEP_QUEUE_SHARDS + 1))
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
        assertThat(progress.getNumberOfShards()).isEqualTo(DEFAULT_SWEEP_QUEUE_SHARDS);
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
        doThrow(new CheckAndSetException("sadness")).when(mockKvs).checkAndSet(any());
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
        doThrow(new CheckAndSetException("sadness")).when(mockKvs).checkAndSet(any());
        ShardProgress instrumentedProgress = new ShardProgress(mockKvs);

        assertThatThrownBy(() -> instrumentedProgress.updateLastSweptTimestamp(CONSERVATIVE_TEN, 12L))
                .isInstanceOf(CheckAndSetException.class);
    }

    private Value createValue(long num) {
        SweepShardProgressTable.Value value = SweepShardProgressTable.Value.of(num);
        return Value.create(value.persistValue(), 0L);
    }
}
