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
import com.palantir.atlasdb.keyvalue.impl.TestResourceManagerV2;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTestV2;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class AbstractMemoryTransactionTest extends AbstractTransactionTestV2 {
    @RegisterExtension
    public static final TestResourceManagerV2 TRM = TestResourceManagerV2.inMemory();

    private static final byte[] ROW_1 = PtBytes.toBytes("row1");
    private static final byte[] ZERO = new byte[0];

    private final int transactionsSchemaVersion;

    public AbstractMemoryTransactionTest(int transactionsSchemaVersion) {
        super(TRM, TRM);
        this.transactionsSchemaVersion = transactionsSchemaVersion;
    }

    @BeforeEach
    public void beforeEach() {
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
