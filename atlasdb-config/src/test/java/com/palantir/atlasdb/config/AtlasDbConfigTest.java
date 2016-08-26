/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.config;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;
import org.mockito.Mockito;

import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public class AtlasDbConfigTest {
    private static final KeyValueServiceConfig KVS_CONFIG = Mockito.mock(KeyValueServiceConfig.class);
    private static final LeaderConfig LEADER_CONFIG = Mockito.mock(LeaderConfig.class);
    private static final ServerListConfig DEFAULT_SERVER_LIST = Mockito.mock(ServerListConfig.class);

    @Test
    public void configWithNoLeaderOrLockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void kvsConfigIsRequired() {
        ImmutableAtlasDbConfig.builder().build();
    }

    @Test
    public void configWithLeaderBlockIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test
    public void remoteLockAndTimestampConfigIsValid() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
        assertThat(config, not(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithLockAndTimestampBlocks() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithLockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void leaderBlockNotPermittedWithTimestampBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(LEADER_CONFIG)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void lockBlockRequiresTimestampBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .lock(DEFAULT_SERVER_LIST)
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void timestampBlockRequiresLockBlock() {
        ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .timestamp(DEFAULT_SERVER_LIST)
                .build();
    }
}
