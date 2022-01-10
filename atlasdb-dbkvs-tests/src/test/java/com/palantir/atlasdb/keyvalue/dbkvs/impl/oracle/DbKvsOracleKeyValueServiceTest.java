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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.AbstractDbKvsKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class DbKvsOracleKeyValueServiceTest extends AbstractDbKvsKeyValueServiceTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsOracleTestSuite::createKvs);

    private static final TableReference TABLE_1 =
            TableReference.createFromFullyQualifiedName("multipass.providerGroupIdAndRealmToPrincipalId");
    private static final TableReference TABLE_2 =
            TableReference.createFromFullyQualifiedName("multipass.providerGroupIdAndRealmToPrincipalId_v2");

    private static final byte[] RAW_TABLE_METADATA = TableMetadata.builder()
            .singleRowComponent("name", ValueType.FIXED_LONG)
            .singleDynamicColumn("name", ValueType.FIXED_LONG, ValueType.FIXED_LONG)
            .conflictHandler(ConflictHandler.IGNORE_ALL)
            .build()
            .persistToBytes();

    private static final byte[] OVERFLOW_TABLE_METADATA = TableMetadata.builder()
            .singleRowComponent("name", ValueType.BLOB)
            .singleDynamicColumn("name", ValueType.BLOB, ValueType.BLOB)
            .conflictHandler(ConflictHandler.IGNORE_ALL)
            .build()
            .persistToBytes();

    public DbKvsOracleKeyValueServiceTest() {
        super(TRM);
    }

    @After
    public void after() {
        keyValueService.dropTables(ImmutableSet.of(TABLE_1, TABLE_2));
    }

    @Override
    @Ignore
    @Test
    public void testGetAllTableNames() {
        // we reuse the KVS, so this test is no longer deterministic
    }

    @Test
    public void shouldCreateMultipleTablesWithAlmostSameHashName() {
        keyValueService.createTable(TABLE_1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE_2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertThat(keyValueService.getAllTableNames()).contains(TABLE_1, TABLE_2);
    }

    @Test
    public void failOnRawToOverflowTableConversion() {
        keyValueService.createTable(TABLE_1, RAW_TABLE_METADATA);
        assertThatThrownBy(() -> keyValueService.createTable(TABLE_1, OVERFLOW_TABLE_METADATA))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageStartingWith("Unsupported table change from raw to overflow");
    }

    @Test
    public void createShouldNotFailOnExistingOverflowTable() {
        keyValueService.createTable(TABLE_1, OVERFLOW_TABLE_METADATA);
        assertThatCode(() -> keyValueService.createTable(TABLE_1, OVERFLOW_TABLE_METADATA))
                .doesNotThrowAnyException();
    }

    @Test
    public void createShouldNotFailOnExistingRawTable() {
        keyValueService.createTable(TABLE_1, RAW_TABLE_METADATA);
        assertThatCode(() -> keyValueService.createTable(TABLE_1, RAW_TABLE_METADATA))
                .doesNotThrowAnyException();
    }

    @Test
    public void overflowToRawConversionSucceeds() {
        keyValueService.createTable(TABLE_1, OVERFLOW_TABLE_METADATA);
        assertThatCode(() -> keyValueService.createTable(TABLE_1, RAW_TABLE_METADATA))
                .doesNotThrowAnyException();
    }
}
