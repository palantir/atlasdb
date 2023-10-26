/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ClusterAvailabilityStatusTest {
    @Test
    public void allAvailableMeansHealthy() {
        assertThat(ClusterAvailabilityStatus.ALL_AVAILABLE.isHealthy()).isTrue();
    }

    @Test
    public void quorumAvailableMeansNotHealthy() {
        assertThat(ClusterAvailabilityStatus.QUORUM_AVAILABLE.isHealthy()).isFalse();
    }

    @Test
    public void noQuorumMeansNotHealthy() {
        assertThat(ClusterAvailabilityStatus.NO_QUORUM_AVAILABLE.isHealthy()).isFalse();
    }

    @Test
    public void terminalMeansNotHealthy() {
        assertThat(ClusterAvailabilityStatus.TERMINAL.isHealthy()).isFalse();
    }
}
