/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import java.util.Collection;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public abstract class AbstractAsyncTimelockServiceIntegrationTest {

    protected static final String LOCALHOST = "https://localhost";
    protected static final String CLIENT = "test";

    protected static final TestableTimelockCluster CLUSTER_WITH_ASYNC = new TestableTimelockCluster(
            LOCALHOST,
            CLIENT,
            "paxosSingleServerWithAsyncLock.yml");
    protected static final TestableTimelockCluster CLUSTER_WITH_ASYNC_CHECK_DISABLED = new TestableTimelockCluster(
            LOCALHOST,
            CLIENT,
            "paxosSingleServerWithAsyncLockCheckDisabled.yml");
    protected static final TestableTimelockCluster CLUSTER_WITH_SYNC_ADAPTER = new TestableTimelockCluster(
            LOCALHOST,
            CLIENT,
            "paxosSingleServerWithSyncLockAdapter.yml");

    @ClassRule
    public static final RuleChain ASYNC_RULE_CHAIN = CLUSTER_WITH_ASYNC.getRuleChain();
    @ClassRule
    public static final RuleChain ASYNC_CHECK_DISABLED_RULE_CHAIN = CLUSTER_WITH_ASYNC_CHECK_DISABLED.getRuleChain();
    @ClassRule
    public static final RuleChain SYNC_ADAPTER_RULE_CHAIN = CLUSTER_WITH_SYNC_ADAPTER.getRuleChain();

    protected final TestableTimelockCluster cluster;

    @Parameterized.Parameters
    public static Collection<TestableTimelockCluster> clusters() {
        return ImmutableList.of(
                CLUSTER_WITH_ASYNC,
                CLUSTER_WITH_ASYNC_CHECK_DISABLED,
                CLUSTER_WITH_SYNC_ADAPTER);
    }

    protected AbstractAsyncTimelockServiceIntegrationTest(TestableTimelockCluster cluster) {
        this.cluster = cluster;
    }

    protected static boolean isUsingSyncAdapter(TestableTimelockCluster cluster) {
        return cluster == CLUSTER_WITH_SYNC_ADAPTER;
    }
}
