/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.timestamp.TimestampService;

public class CassandraTimestampIntegrationTest {
    private CassandraKeyValueService kv;

    @Before
    public void setUp() {
        kv = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.cassandraKvsConfig),
                CassandraTestSuite.leaderConfig);
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @After
    public void close() {
        kv.close();
    }

    @Test
    public void testBounds() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        ts.storeUpperLimit(limit + 10);
        Assert.assertEquals(limit + 10, ts.getUpperLimit());
        ts.storeUpperLimit(limit + 20);
        Assert.assertEquals(limit + 20, ts.getUpperLimit());
        ts.storeUpperLimit(limit + 30);
        Assert.assertEquals(limit + 30, ts.getUpperLimit());
    }

    @Test
    public void testMultipleThrows() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        TimestampBoundStore ts2 = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        Assert.assertEquals(limit, ts2.getUpperLimit());
        ts.storeUpperLimit(limit + 10);
        Assert.assertEquals(limit + 10, ts.getUpperLimit());
        Assert.assertEquals(limit + 10, ts2.getUpperLimit());

        ts.storeUpperLimit(limit + 20);
        try {
            ts2.storeUpperLimit(limit + 20);
            fail();
        } catch (MultipleRunningTimestampServiceError e) {
            // expected
        }
        Assert.assertEquals(limit + 20, ts.getUpperLimit());
        Assert.assertEquals(limit + 20, ts2.getUpperLimit());

        ts.storeUpperLimit(limit + 30);
        Assert.assertEquals(limit + 30, ts.getUpperLimit());

        try {
            ts2.storeUpperLimit(limit + 40);
            fail();
        } catch (MultipleRunningTimestampServiceError e) {
            // expected
        }
    }

    @Test
    public void testQuorumStuff() throws InterruptedException {
        LeaderElectionService leadership1 = mock(LeaderElectionService.class);
        LeaderElectionService leadership2 = mock(LeaderElectionService.class);

        AtomicInteger leaderIdx = new AtomicInteger(1);

        when(leadership1.blockOnBecomingLeader()).thenAnswer((inv) -> {
            waitUntilLeader(leaderIdx, 1);
            return mock(LeaderElectionService.LeadershipToken.class);
        });

        when(leadership2.blockOnBecomingLeader()).thenAnswer((inv) -> {
            waitUntilLeader(leaderIdx, 2);
            return mock(LeaderElectionService.LeadershipToken.class);
        });

        when(leadership1.getSuspectedLeaderInMemory()).thenReturn(Optional.absent());
        when(leadership2.getSuspectedLeaderInMemory()).thenReturn(Optional.absent());

        TimestampService ts = AwaitingLeadershipProxy.newProxyInstance(
                TimestampService.class,
                () -> PersistentTimestampService.create(CassandraTimestampBoundStore.create(kv)),
                leadership1);
        TimestampService ts2 = AwaitingLeadershipProxy.newProxyInstance(
                TimestampService.class,
                () -> PersistentTimestampService.create(CassandraTimestampBoundStore.create(kv)),
                leadership2);

        //====== SET FIRST TS SERVICE AS LEADER
        leaderIdx.set(1);
        when(leadership1.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.LEADING);
        Thread.sleep(1000);

        ts.getFreshTimestamp();
        try {
            ts2.getFreshTimestamp();
            fail();
        } catch (NotCurrentLeaderException e) {
            // This should fail
        }

        //======= SET SECOND TS SERVICE AS LEADER. DO NOT QUERY FIRST TS SERVICE
        leaderIdx.set(2);
        when(leadership1.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.NOT_LEADING);
        when(leadership2.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.LEADING);
        Thread.sleep(1000);

        ts2.getFreshTimestamp();

        //======= SET FIRST TS SERVICE AS LEADER. MAKE SURE TO QUERY DATABASE
        leaderIdx.set(1);
        when(leadership1.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.LEADING);
        when(leadership2.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        ts.getFreshTimestamps(100000000);
        ts.getFreshTimestamp();
    }

    private static void waitUntilLeader(AtomicInteger leaderIdx, int wantedIdx) throws InterruptedException {
        while (leaderIdx.get() != wantedIdx) {
            Thread.sleep(10);
        }
    }
}
