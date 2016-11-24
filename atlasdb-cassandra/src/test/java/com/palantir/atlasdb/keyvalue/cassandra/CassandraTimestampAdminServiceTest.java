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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;

import org.apache.cassandra.thrift.Column;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class CassandraTimestampAdminServiceTest {
    private CassandraKeyValueService mockCassandraKvs;
    private CassandraTimestampAdminService adminService;

    @Before
    public void setUp() {
        mockCassandraKvs = mock(CassandraKeyValueService.class);
        adminService = new CassandraTimestampAdminService(mockCassandraKvs);
    }

    @Test
    public void canDetermineIfTryingToFastForwardToThePast() {
        assertThat(CassandraTimestampAdminService.fastForwardingToThePast(5L, Optional.of(0L)), is(false));
        assertThat(CassandraTimestampAdminService.fastForwardingToThePast(5L, Optional.of(10L)), is(true));
    }

    @Test
    public void fastForwardingWithNoPastDataIsNotFastForwardingToThePast() {
        assertThat(CassandraTimestampAdminService.fastForwardingToThePast(Long.MIN_VALUE, Optional.empty()), is(false));
    }

    @Test
    public void shouldRefuseToCreateAdminServiceGivenNonCassandraKvs() {
        KeyValueService otherKvs = mock(KeyValueService.class);
        try {
            new CassandraTimestampAdminService(otherKvs);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void repairTableRecreatesTimestampTableWithCorrectMetadata() {
        adminService.repairTable();
        verify(mockCassandraKvs, times(1)).createTable(eq(AtlasDbConstants.TIMESTAMP_TABLE),
                aryEq(CassandraTimestampConstants.TIMESTAMP_TABLE_METADATA.persistToBytes()));
    }

    @Test
    public void bogusColumnShouldNotBeReadableAsLong() {
        Column column = adminService.makeBogusColumn();
        try {
            PtBytes.toLong(column.getValue());
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
