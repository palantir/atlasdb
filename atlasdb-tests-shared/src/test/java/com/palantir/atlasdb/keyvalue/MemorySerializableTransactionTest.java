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

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MemorySerializableTransactionTest extends AbstractSerializableTransactionTest {
    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

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

    public MemorySerializableTransactionTest(String name, int transactionsSchemaVersion) {
        super(TRM, TRM);
        this.transactionsSchemaVersion = transactionsSchemaVersion;
    }

    @Before
    public void before() {
        keyValueService.truncateTable(AtlasDbConstants.COORDINATION_TABLE);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                transactionSchemaManager, timestampService, timestampManagementService, transactionsSchemaVersion);
    }
}
