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
package com.palantir.atlasdb.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MemoryTransactionTest extends AbstractTransactionTest {
    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    private static final byte[] ROW_1 = PtBytes.toBytes("row1");
    private static final byte[] ZERO = new byte[0];

    private static final String TRANSACTIONS_1 = "transactions1";
    private static final String TRANSACTIONS_3 = "transactions3";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
            {TRANSACTIONS_1, TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION},
            {TRANSACTIONS_3, TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION}
        };
        return Arrays.asList(data);
    }

    private final int transactionsSchemaVersion;

    public MemoryTransactionTest(String name, int transactionsSchemaVersion) {
        super(TRM, TRM);
        this.transactionsSchemaVersion = transactionsSchemaVersion;
    }

    @Before
    public void before() {
        keyValueService.truncateTable(AtlasDbConstants.COORDINATION_TABLE);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                transactionSchemaManager, timestampService, timestampManagementService, transactionsSchemaVersion);
    }

    @Test
    public void testKeyValueRangeColumnSelectionEndInclusive() {
        setup();
        assertThat(keyValueService.getRowKeysInRange(TEST_TABLE, ZERO, ROW_1, 9))
                .containsExactly(ROW_1);
    }

    @Test
    public void testKeyValueRangeColumnSelectionEntireTable() {
        setup();
        byte[] row1 = PtBytes.toBytes("row1");
        assertThat(keyValueService.getRowKeysInRange(TEST_TABLE, ZERO, ZERO, 9))
                .containsExactly(row1, PtBytes.toBytes("row1a"), PtBytes.toBytes("row2"));
    }

    @Test
    public void testKeyValueRangeColumnSelectionStartInclusive() {
        setup();
        byte[] row1 = PtBytes.toBytes("row1");
        assertThat(keyValueService.getRowKeysInRange(TEST_TABLE, ROW_1, ZERO, 9))
                .containsExactly(row1, PtBytes.toBytes("row1a"), PtBytes.toBytes("row2"));
    }

    @Test
    public void testKeyValueRangeColumnSelectionMaxResults() {
        setup();
        byte[] row1 = PtBytes.toBytes("row1");
        assertThat(keyValueService.getRowKeysInRange(TEST_TABLE, ZERO, ZERO, 2))
                .containsExactly(row1, PtBytes.toBytes("row1a"));
    }

    private void setup() {
        putDirect("row1", "col1", "v1", 0);
        putDirect("row1", "col2", "v2", 2);
        putDirect("row1", "col4", "v5", 3);
        putDirect("row1a", "col4", "v5", 100);
        putDirect("row2", "col2", "v3", 1);
        putDirect("row2", "col4", "v4", 6);
    }
}
