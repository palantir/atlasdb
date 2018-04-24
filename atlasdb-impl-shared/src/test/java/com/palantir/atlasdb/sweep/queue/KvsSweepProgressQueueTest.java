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

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class KvsSweepProgressQueueTest {
    private KeyValueService kvs;
    private KvsSweepQueueProgress progress;

    @Before
    public void setup() {
        kvs = new InMemoryKeyValueService(true);
        progress = new KvsSweepQueueProgress(kvs);
    }

    @Test
    public void canReadInitialNumberOfShards() {
        assertThat(progress.getNumberOfShards()).isEqualTo(1);
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
        assertThat(progress.getLastSweptTimestampPartition(10, true)).isEqualTo(-1L);
    }

    @Test
    public void canUpdateSweptTimestamp() {
        progress.updateLastSweptTimestampPartition(10, true, 1024L);
        assertThat(progress.getLastSweptTimestampPartition(10, true)).isEqualTo(1024L);
    }

    @Test
    public void attemptingToDecreaseSweptTimestampIsNoop() {
        progress.updateLastSweptTimestampPartition(10, true, 1024L);
        progress.updateLastSweptTimestampPartition(10, true, 512L);
        assertThat(progress.getLastSweptTimestampPartition(10, true)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneShardDoesNotAffectOthers() {
        progress.updateLastSweptTimestampPartition(10, true, 1024L);
        assertThat(progress.getLastSweptTimestampPartition(20, true)).isEqualTo(-1L);

        progress.updateLastSweptTimestampPartition(20, true, 512L);
        assertThat(progress.getLastSweptTimestampPartition(20, true)).isEqualTo(512L);
        assertThat(progress.getLastSweptTimestampPartition(10, true)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneConsistencyDoesNotAffectOther() {
        progress.updateLastSweptTimestampPartition(10, true, 128L);
        assertThat(progress.getLastSweptTimestampPartition(10, false)).isEqualTo(-1L);

        progress.updateLastSweptTimestampPartition(10, false, 32L);
        assertThat(progress.getLastSweptTimestampPartition(10, true)).isEqualTo(128L);
        assertThat(progress.getLastSweptTimestampPartition(10, false)).isEqualTo(32L);
    }

    @Test
    public void updatingTimestampsDoesNotAffectShardsAndViceVersa() {
        progress.updateNumberOfShards(64);
        progress.updateLastSweptTimestampPartition(10, true, 32L);
        progress.updateLastSweptTimestampPartition(10, false, 128L);

        assertThat(progress.getNumberOfShards()).isEqualTo(64);
        assertThat(progress.getLastSweptTimestampPartition(10, true)).isEqualTo(32L);
        assertThat(progress.getLastSweptTimestampPartition(10, false)).isEqualTo(128L);
    }
}
