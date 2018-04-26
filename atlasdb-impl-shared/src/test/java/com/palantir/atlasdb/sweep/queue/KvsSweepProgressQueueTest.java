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

import static com.palantir.atlasdb.sweep.queue.KvsSweepQueueProgress.INITIAL_SHARDS;
import static com.palantir.atlasdb.sweep.queue.KvsSweepQueueProgress.INITIAL_TIMESTAMP;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class KvsSweepProgressQueueTest {
    private KeyValueService kvs;
    private KvsSweepQueueProgress progress;

    private static final ShardAndStrategy CONSERVATIVE_TEN = ShardAndStrategy.conservative(10);
    private static final ShardAndStrategy THOROUGH_TEN = ShardAndStrategy.thorough(10);
    private static final ShardAndStrategy CONSERVATIVE_TWENTY = ShardAndStrategy.conservative(20);

    @Before
    public void setup() {
        kvs = new InMemoryKeyValueService(true);
        progress = new KvsSweepQueueProgress(kvs);
    }

    @Test
    public void canReadInitialNumberOfShards() {
        assertThat(progress.getNumberOfShards()).isEqualTo(INITIAL_SHARDS);
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
    public void increasingNumberOfShardsAbove128Throws() {
        assertThatThrownBy(() -> progress.updateNumberOfShards(256)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canReadInitialSweptTimestamp() {
        assertThat(progress.getLastSweptTimestampPartition(ShardAndStrategy.conservative(10))).isEqualTo(INITIAL_TIMESTAMP);
    }

    @Test
    public void canUpdateSweptTimestamp() {
        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TEN, 1024L);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(1024L);
    }

    @Test
    public void attemptingToDecreaseSweptTimestampIsNoop() {
        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TEN, 1024L);
        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TEN, 512L);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneShardDoesNotAffectOthers() {
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TWENTY)).isEqualTo(INITIAL_TIMESTAMP);
        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TEN, 1024L);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TWENTY)).isEqualTo(INITIAL_TIMESTAMP);

        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TWENTY, 512L);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TWENTY)).isEqualTo(512L);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneConsistencyDoesNotAffectOther() {
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        assertThat(progress.getLastSweptTimestampPartition(THOROUGH_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TEN, 128L);
        assertThat(progress.getLastSweptTimestampPartition(THOROUGH_TEN)).isEqualTo(INITIAL_TIMESTAMP);

        progress.updateLastSweptTimestampPartition(THOROUGH_TEN, 32L);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(128L);
        assertThat(progress.getLastSweptTimestampPartition(THOROUGH_TEN)).isEqualTo(32L);
    }

    @Test
    public void updatingTimestampsDoesNotAffectShardsAndViceVersa() {
        assertThat(progress.getNumberOfShards()).isEqualTo(INITIAL_SHARDS);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(INITIAL_TIMESTAMP);
        assertThat(progress.getLastSweptTimestampPartition(THOROUGH_TEN)).isEqualTo(INITIAL_TIMESTAMP);

        progress.updateNumberOfShards(64);
        progress.updateLastSweptTimestampPartition(CONSERVATIVE_TEN, 32L);
        progress.updateLastSweptTimestampPartition(THOROUGH_TEN, 128L);

        assertThat(progress.getNumberOfShards()).isEqualTo(64);
        assertThat(progress.getLastSweptTimestampPartition(CONSERVATIVE_TEN)).isEqualTo(32L);
        assertThat(progress.getLastSweptTimestampPartition(THOROUGH_TEN)).isEqualTo(128L);
    }
}
