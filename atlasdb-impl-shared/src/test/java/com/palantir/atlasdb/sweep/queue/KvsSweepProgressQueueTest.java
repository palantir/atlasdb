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
        assertThat(progress.numberOfShards()).isEqualTo(1);
    }

    @Test
    public void canUpdateNumberOfShards() {
        progress.numberOfShards();
        progress.updateNumberOfShards(128);
        assertThat(progress.numberOfShards()).isEqualTo(128);
    }

    @Test
    public void attemptingToDecreaseNumberOfShardsIsNoop() {
        progress.numberOfShards();
        progress.updateNumberOfShards(64);
        progress.updateNumberOfShards(32);
        assertThat(progress.numberOfShards()).isEqualTo(64);
    }

    @Test
    public void increasingNumberOfShardsAbove128Throws() {
        progress.numberOfShards();
        assertThatThrownBy(() -> progress.updateNumberOfShards(256)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canReadInitialSweptTimestamp() {
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(0L);
    }

    @Test
    public void canUpdateSweptTimestamp() {
        progress.lastSweptTimestamp(10, true);
        progress.updateLastSweptTimestamp(10, true, 1024L);
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(1024L);
    }

    @Test
    public void attemptingToDecreaseSweptTimestampIsNoop() {
        progress.lastSweptTimestamp(10, true);
        progress.updateLastSweptTimestamp(10, true, 1024L);
        progress.updateLastSweptTimestamp(10, true, 512L);
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneShardDoesNotAffectOthers() {
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(0L);
        assertThat(progress.lastSweptTimestamp(20, true)).isEqualTo(0L);
        progress.updateLastSweptTimestamp(10, true, 1024L);
        assertThat(progress.lastSweptTimestamp(20, true)).isEqualTo(0L);

        progress.updateLastSweptTimestamp(20, true, 512L);
        assertThat(progress.lastSweptTimestamp(20, true)).isEqualTo(512L);
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(1024L);
    }

    @Test
    public void updatingTimestampForOneConsistencyDoesNotAffectOther() {
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(0L);
        assertThat(progress.lastSweptTimestamp(10, false)).isEqualTo(0L);
        progress.updateLastSweptTimestamp(10, true, 128L);
        assertThat(progress.lastSweptTimestamp(10, false)).isEqualTo(0L);

        progress.updateLastSweptTimestamp(10, false, 32L);
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(128L);
        assertThat(progress.lastSweptTimestamp(10, false)).isEqualTo(32L);
    }

    @Test
    public void updatingTimestampsDoesNotAffectShardsAndViceVersa() {
        assertThat(progress.numberOfShards()).isEqualTo(1);
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(0L);
        assertThat(progress.lastSweptTimestamp(10, false)).isEqualTo(0L);

        progress.updateNumberOfShards(64);
        progress.updateLastSweptTimestamp(10, true, 32L);
        progress.updateLastSweptTimestamp(10, false, 128L);

        assertThat(progress.numberOfShards()).isEqualTo(64);
        assertThat(progress.lastSweptTimestamp(10, true)).isEqualTo(32L);
        assertThat(progress.lastSweptTimestamp(10, false)).isEqualTo(128L);
    }
}
