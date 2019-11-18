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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class NonBlockingAppenderIntegrationTest {
    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "https://localhost",
            "paxosSingleServerWithNonBlockingAppender.yml");

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @BeforeClass
    public static void setUp() {
        CLUSTER.waitUntilLeaderIsElected();
    }

    @Test
    public void canDeserializeConfigAndStart() {
        CLUSTER.clientForRandomNamespace().getFreshTimestamp();
    }

}
