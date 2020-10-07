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

import java.io.IOException;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.palantir.atlasdb.timelock.suite.SingleLeaderPaxosSuite;
import com.palantir.atlasdb.timelock.util.ParameterInjector;

@RunWith(Parameterized.class)
public class DbKvsMNPTLIT extends AbstractTests {
    private boolean trial = false;

    @ClassRule
    public static ParameterInjector<ClusterSupplier> injector =
            ParameterInjector
                    .withFallBackConfiguration(() -> ClusterSupplier.of(() -> SingleLeaderPaxosSuite.BATCHED_PAXOS));

    @Parameterized.Parameter
    public ClusterSupplier cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<ClusterSupplier> params() {
        return injector.getParameter();
    }

    @Override
    protected TestableTimelockCluster getFirstCluster() {
        return params().iterator().next().getCluster();
    }

    @Override
    protected TestableTimelockCluster getCluster() {
        TestableTimelockCluster c = cluster.getCluster();
        setupBeforeClass(c);
        return c;
    }

    private void setupBeforeClass(TestableTimelockCluster c) {
        if (trial) return;
        trial = true;
        try {
            c.temporaryFolder.create();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (TemporaryConfigurationHolder config : c.getConfigs()) {
            try {
                config.before();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
