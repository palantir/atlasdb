/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.config.ClusterConfiguration;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

public class BlockingTimeoutsTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String ADDRESS = "localhost:8701";
    private static final ClusterConfiguration CLUSTER = ImmutableClusterConfiguration.builder()
            .localServer(ADDRESS)
            .addServers(ADDRESS)
            .build();
    private static final Set<String> CLIENTS = ImmutableSet.of("client1", "client2");

    private static final long DEFAULT_BLOCKING_TIMEOUT =
            BlockingTimeouts.scaleForErrorMargin(BlockingTimeouts.DEFAULT_IDLE_TIMEOUT);

    private static final long ONE_MILLION = 1_000_000;
    private static final long FIVE_HUNDRED = 500;
    private static final long FIVE = 5;

    @Test
    public void scaleForErrorMarginReducesTimeoutSlightly() {
        assertThat(BlockingTimeouts.scaleForErrorMargin(ONE_MILLION))
                .isLessThan(ONE_MILLION)
                .isEqualTo(970_000);
    }

    @Test
    public void scaleForErrorMarginScalesProportionalToTimeoutValue() {
        assertThat(BlockingTimeouts.scaleForErrorMargin(FIVE_HUNDRED))
                .isGreaterThan(0)
                .isEqualTo(485);
        assertThat(BlockingTimeouts.scaleForErrorMargin(FIVE))
                .isGreaterThan(0)
                .isEqualTo(FIVE);
    }

    @Test
    public void returnsDefaultBlockingTimeoutWithNoSpecifiedConnectors() {
        TimeLockServerConfiguration basicConfiguration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS);
        assertThat(BlockingTimeouts.getBlockingTimeout(OBJECT_MAPPER, basicConfiguration))
                .isEqualTo(DEFAULT_BLOCKING_TIMEOUT);
    }
}
