/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.util;

public enum TestableTimeLockClusterPorts {
    LOCK_WATCH_EVENT_INTEGRATION_TEST(9999),
    LOCK_WATCH_VALUE_INTEGRATION_TEST(9010),
    DB_TIMELOCK_SINGLE_LEADER_PAXOS_STRESS_TESTS(9020),
    MULTI_LEADER_PAXOS_STRESS_TESTS(9030),
    SINGLE_LEADER_PAXOS_STRESS_TESTS_1(9040),
    SINGLE_LEADER_PAXOS_STRESS_TESTS_2(9050),
    SINGLE_LEADER_PAXOS_STRESS_TESTS_3(9060),
    DB_TIMELOCK_SINGLE_LEADER_PAXOS_SUITE(9070),
    MULTI_LEADER_PAXOS_SUITE(9080),
    SINGLE_LEADER_PAXOS_SUITE_1(9090),
    SINGLE_LEADER_PAXOS_SUITE_2(9100),
    SINGLE_LEADER_PAXOS_SUITE_3(9110);

    private final int startingPort;

    TestableTimeLockClusterPorts(int startingPort) {
        this.startingPort = startingPort;
    }

    public int getStartingPort() {
        return startingPort;
    }
}
