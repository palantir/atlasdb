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
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;

public class LockTableServiceTest {
    private LockTableService lockTableService;

    @Before
    public void setup() {
        CassandraKeyValueServiceConfigManager configManager = mock(CassandraKeyValueServiceConfigManager.class);
        CassandraClientPool clientPool = mock(CassandraClientPool.class);
        lockTableService = new LockTableService(configManager, clientPool);
    }

    @Test
    public void shouldReturnConstantLockTableReference() {
        assertThat(lockTableService.getLockTable().getTablename(), is("_locks"));
    }

    @Test
    public void lockTableShouldBeInSetOfAllLockTables() {
        assertThat(lockTableService.getAllLockTables(), hasItem(lockTableService.getLockTable()));
    }
}