/**
 * Copyright 2017 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampStoreInvalidatorIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampIntegrationTest.class)
            .with(new CassandraContainer());

    private static final long ONE_MILLION = 1_000_000;

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);
    private final CassandraTimestampStoreInvalidator invalidator = new CassandraTimestampStoreInvalidator(
            new CassandraTimestampBackupRunner(kv));

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampUtils.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    @After
    public void close() {
        kv.close();
    }

    @Test
    public void canBackupTimestampTableIfItDoesNotExist() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupTimestampTableIfItExistsWithNoData() {
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupTwice() {
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupTimestampTableIfItExistsWithData() {
        TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        long limit = timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(limit + ONE_MILLION);
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(limit + ONE_MILLION);
    }

    @Test
    public void canBackupAndRestoreTimestampTable() {
        TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        long limit = timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(limit + ONE_MILLION);
        invalidator.backupAndInvalidate();
        invalidator.revalidateFromBackup();
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(limit + ONE_MILLION);
    }

    @Test
    public void restoringValidTimestampTableIsANoOp() {
        checkThatWeCanReadInitialValue();
        invalidator.revalidateFromBackup();
        checkThatWeCanReadInitialValue();
        invalidator.revalidateFromBackup();
        checkThatWeCanReadInitialValue();
    }

    @Test
    public void restoringTimestampTableIfItDoesNotExistIsANoOp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        invalidator.revalidateFromBackup();
        checkThatWeCanReadInitialValue();
    }

    @Test
    public void restoringTimestampTableWithNoDataIsANoOp() {
        invalidator.revalidateFromBackup();
        checkThatWeCanReadInitialValue();
    }

    private void checkThatWeCanReadInitialValue() {
        TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }
}
