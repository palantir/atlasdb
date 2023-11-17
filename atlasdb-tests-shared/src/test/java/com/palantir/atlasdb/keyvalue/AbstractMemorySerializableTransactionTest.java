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
import com.palantir.atlasdb.keyvalue.impl.TestResourceManagerV2;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTestV2;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class AbstractMemorySerializableTransactionTest extends AbstractSerializableTransactionTestV2 {
    @RegisterExtension
    public static final TestResourceManagerV2 TRM = TestResourceManagerV2.inMemory();

    private final int transactionsSchemaVersion;

    public AbstractMemorySerializableTransactionTest(int transactionsSchemaVersion) {
        super(TRM, TRM);
        this.transactionsSchemaVersion = transactionsSchemaVersion;
    }

    @BeforeEach
    public void beforeEach() {
        keyValueService.truncateTable(AtlasDbConstants.COORDINATION_TABLE);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                transactionSchemaManager, timestampService, timestampManagementService, transactionsSchemaVersion);
    }
}
