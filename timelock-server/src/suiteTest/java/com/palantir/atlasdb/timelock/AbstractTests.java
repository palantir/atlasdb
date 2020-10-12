/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.http.v2.ClientOptionsConstants;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;

public abstract class AbstractTests {
    protected NamespacedClients client;

    @ClassRule
    public static TestableTimelockCluster cluster;

    protected static TestableTimelockCluster FIRST_CLUSTER;// = params().iterator().next();
    private static final LockDescriptor LOCK = StringLockDescriptor.of("foo");
    private static final Set<LockDescriptor> LOCKS = ImmutableSet.of(LOCK);

    private static final int DEFAULT_LOCK_TIMEOUT_MS = 10_000;
    private static final int LONGER_THAN_READ_TIMEOUT_LOCK_TIMEOUT_MS =
            Ints.saturatedCast(ClientOptionsConstants.SHORT_READ_TIMEOUT
                    .toJavaDuration()
                    .plus(Duration.ofSeconds(1)).toMillis());

    @Before
    public void setUp() throws Exception {
        cluster = getCluster();
        client = getClient();
        readyCluster();
        FIRST_CLUSTER = getFirstCluster();
    }

    private void readyCluster() {
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));
    }

    protected NamespacedClients getClient() {
        return cluster.clientForRandomNamespace().throughWireMockProxy();
    }

    protected void bringAllNodesOnline() {
        client = cluster.clientForRandomNamespace().throughWireMockProxy();
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));
    }

    protected abstract TestableTimelockCluster getFirstCluster();
    protected abstract TestableTimelockCluster getCluster();

    @Test
    public void timestampsAreIncreasingAcrossFailovers() {
        long lastTimestamp = client.getFreshTimestamp();

        for (int i = 0; i < 3; i++) {
            cluster.failoverToNewLeader(client.namespace());

            long timestamp = client.getFreshTimestamp();
            assertThat(timestamp).isGreaterThan(lastTimestamp);
            lastTimestamp = timestamp;
        }
    }
}
