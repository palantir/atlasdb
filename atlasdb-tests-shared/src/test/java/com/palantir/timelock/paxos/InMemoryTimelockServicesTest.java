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

package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class InMemoryTimelockServicesTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private InMemoryTimelockServices inMemoryTimelockServices;

    private TimestampService timestampService;
    private AsyncTimelockService timelockService;
    private TimelockService delegatingTimelockService;

    @Before
    public void setup() {
        inMemoryTimelockServices = InMemoryTimelockServices.create(tempFolder);
        timestampService = inMemoryTimelockServices.getTimestampService();
        timelockService = inMemoryTimelockServices.getTimelockService();
        delegatingTimelockService = inMemoryTimelockServices.getLegacyTimelockService();

        Awaitility.await()
                .atMost(Duration.ofSeconds(10L))
                .pollInterval(Duration.ofSeconds(1L))
                .ignoreExceptions()
                .until(() -> timestampService.getFreshTimestamp() > 0);
    }

    @After
    public void tearDown() {
        inMemoryTimelockServices.close();
    }

    @Test
    public void canGetTimestamp() {
        long ts1 = timestampService.getFreshTimestamp();
        long ts2 = timelockService.getFreshTimestamp();
        testTs(ts1, ts2);
    }

    @Test
    public void timestampsAreConsistent() {
        long ts1 = delegatingTimelockService.getFreshTimestamps(1).getLowerBound();
        long ts2 = timestampService.getFreshTimestamp();
        testTs(ts1, ts2);

        long ts3 = delegatingTimelockService.getFreshTimestamps(1).getLowerBound();
        testTs(ts2, ts3);

        long ts4 = delegatingTimelockService
                .startIdentifiedAtlasDbTransactionBatch(1)
                .get(0)
                .startTimestampAndPartition()
                .timestamp();
        testTs(ts3, ts4);

        long ts5 = delegatingTimelockService.getFreshTimestamp();
        testTs(ts4, ts5);
    }

    private void testTs(long ts2, long ts3) {
        assertThat(ts2).isLessThan(ts3);
    }

    @Test
    public void canFastForwardTimestamp() {
        long target = 1234567L;
        timelockService.fastForwardTimestamp(target);
        long ts1 = timestampService.getFreshTimestamp();
        assertThat(ts1).isGreaterThan(target);
    }
}
