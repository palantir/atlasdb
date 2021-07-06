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
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import org.junit.Test;

public class ServiceExposureEteTest {
    @Test
    public void shouldExposeATimestampServer() {
        TimestampService timestampClient = EteSetup.createClientToAllNodes(TimestampService.class);

        assertThat(timestampClient.getFreshTimestamp()).isNotNull();
    }

    @Test
    public void shouldExposeATimestampManagementServer() {
        TimestampService timestampClient = EteSetup.createClientToAllNodes(TimestampService.class);
        TimestampManagementService timestampManagementClient =
                EteSetup.createClientToAllNodes(TimestampManagementService.class);

        assertThat(timestampClient.getFreshTimestamp()).isNotNull();

        long newts = timestampClient.getFreshTimestamp() + 10000000;
        timestampManagementClient.fastForwardTimestamp(newts);

        assertThat(timestampClient.getFreshTimestamp()).isGreaterThan(newts);
    }
}
