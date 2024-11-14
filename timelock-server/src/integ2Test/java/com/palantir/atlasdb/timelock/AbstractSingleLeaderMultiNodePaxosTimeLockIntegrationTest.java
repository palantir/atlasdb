/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractSingleLeaderMultiNodePaxosTimeLockIntegrationTest {

    private final TestableTimelockCluster cluster;

    private NamespacedClients namespace;

    public AbstractSingleLeaderMultiNodePaxosTimeLockIntegrationTest(TestableTimelockCluster cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setUp() {
        namespace = cluster.clientForRandomNamespace().throughWireMockProxy();
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(namespace.namespace()));
    }

    @Test
    public void clientsCreatedDynamicallyOnNonLeadersAreFunctionalAfterFailover() {
        cluster.nonLeaders(namespace.namespace()).forEach((clientName, server) -> assertThatThrownBy(
                        () -> server.client(clientName).getFreshTimestamp())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound));

        cluster.failoverToNewLeader(namespace.namespace());

        namespace.getFreshTimestamp();
    }

    @Test
    public void clientsCreatedDynamicallyOnLeaderAreFunctionalImmediately() {
        assertThatCode(() -> cluster.currentLeaderFor(namespace.namespace())
                        .client(namespace.namespace())
                        .getFreshTimestamp())
                .doesNotThrowAnyException();
    }

    @Test
    public void noConflictIfLeaderAndNonLeadersSeparatelyInitializeClient() {
        cluster.nonLeaders(namespace.namespace()).forEach((clientName, server) -> assertThatThrownBy(
                        () -> server.client(clientName).getFreshTimestamp())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound));

        long ts1 = namespace.getFreshTimestamp();

        cluster.failoverToNewLeader(namespace.namespace());

        long ts2 = namespace.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
    }
}
