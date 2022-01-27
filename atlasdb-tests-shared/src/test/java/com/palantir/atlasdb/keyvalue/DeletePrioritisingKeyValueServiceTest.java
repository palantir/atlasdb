/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMultimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.impl.DeletePrioritisingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DeletePrioritisingKeyValueServiceTest extends AbstractSerializableTransactionTest {
    @ClassRule
    public static final TestResourceManager TRM =
            new TestResourceManager(() -> new DeletePrioritisingKeyValueService(new InMemoryKeyValueService(false)));

    private static final String TRANSACTIONS_3 = "transactions3";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data =
                new Object[][] {{TRANSACTIONS_3, TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION}};
        return Arrays.asList(data);
    }

    private final int transactionsSchemaVersion;

    public DeletePrioritisingKeyValueServiceTest(String name, int transactionsSchemaVersion) {
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
    public void detectsFailureToReadWrites() {
        try {
            ((DeletePrioritisingKeyValueService) keyValueService).setPrioritiseDeletes(true);
            Transaction t0 = startTransaction();
            put(t0, "row1", "col1", "100");
            put(t0, "row2", "col1", "100");
            t0.commit();

            Transaction t1 = startTransaction();
            withdrawMoney(t1, true, false);

            keyValueService.delete(
                    TEST_TABLE_SERIALIZABLE, ImmutableMultimap.of(createCell("row1", "col1"), t1.getTimestamp()));

            assertThatThrownBy(t1::commit)
                    .isInstanceOf(SafeIllegalStateException.class)
                    .hasMessageContaining(
                            "We did not read our own writes but still held the immutable timestamp " + "lock!");
        } finally {
            ((DeletePrioritisingKeyValueService) keyValueService).setPrioritiseDeletes(false);
        }
    }

    private Cell createCell(String rowName, String columnName) {
        return Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
    }
}
