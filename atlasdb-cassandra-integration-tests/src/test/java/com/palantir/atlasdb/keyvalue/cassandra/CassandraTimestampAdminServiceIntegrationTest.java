/**
 * Copyright 2016 Palantir Technologies
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.timestamp.TimestampAdminService;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampAdminServiceIntegrationTest {
    private static final long TIMESTAMP_1 = 3141592;
    private static final long TIMESTAMP_2 = 31415926;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampIntegrationTest.class)
            .with(new CassandraContainer());

    private CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);
    private TimestampAdminService adminService;
    private TimestampBoundStore boundStore;

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        adminService = new CassandraTimestampAdminService(kv);
        boundStore = CassandraTimestampBoundStore.create(kv);
    }

    @After
    public void close() {
        kv.close();
    }

    @Test
    public void throwsIfRetrievingUpperBoundWithoutTimestampTable() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        try {
            adminService.getUpperBoundTimestamp();
            fail();
        } catch (PalantirRuntimeException e) {
            if (!(e.getCause() instanceof InvalidRequestException)) {
                fail();
            }
            // expected
        }
    }

    @Test
    public void canGetUpperBound() {
        boundStore.getUpperLimit(); // weird invariant of TimestampBoundStore; must get at least once before a store
        boundStore.storeUpperLimit(TIMESTAMP_1);
        assertThat(adminService.getUpperBoundTimestamp(), greaterThanOrEqualTo(TIMESTAMP_1));
    }

    @Test
    public void fastForwardAffectsFutureTimestamps() {
        System.out.println(adminService.getUpperBoundTimestamp());
        adminService.fastForwardTimestamp(TIMESTAMP_1);
        System.out.println(adminService.getUpperBoundTimestamp());
        assertThat(boundStore.getUpperLimit(), greaterThanOrEqualTo(TIMESTAMP_1));
    }

    @Test
    public void fastForwardWorksEvenWithoutTimestampTable() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        adminService.fastForwardTimestamp(TIMESTAMP_1);
        assertThat(boundStore.getUpperLimit(), greaterThanOrEqualTo(TIMESTAMP_1));
    }

    @Test
    public void fastForwardToThePastDoesNothing() {
        adminService.fastForwardTimestamp(TIMESTAMP_2);
        assertThat(adminService.getUpperBoundTimestamp(), is(TIMESTAMP_2));
        adminService.fastForwardTimestamp(TIMESTAMP_1);
        assertThat(adminService.getUpperBoundTimestamp(), is(TIMESTAMP_2));
    }

    @Test
    public void cannotReadTimestampsAfterInvalidation() {
        adminService.invalidateTimestamps();
        try {
            boundStore.getUpperLimit();
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void canReadTimestampsAfterInvalidationAndFastForward() {
        adminService.invalidateTimestamps();
        adminService.fastForwardTimestamp(TIMESTAMP_1);
        assertThat(boundStore.getUpperLimit(), greaterThanOrEqualTo(TIMESTAMP_1));
    }
}
