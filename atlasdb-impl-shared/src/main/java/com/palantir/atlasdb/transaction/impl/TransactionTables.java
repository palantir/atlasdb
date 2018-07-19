/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public final class TransactionTables {
    private TransactionTables() {
        // Utility class
    }

    public static void createTables(KeyValueService keyValueService) {
        keyValueService.createTables(ImmutableMap.of(
                TransactionConstants.TRANSACTION_TABLE,
                TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes(),
                TransactionConstants.TRANSACTION_TABLE_V2,
                TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes()));
    }

    public static void deleteTables(KeyValueService keyValueService) {
        keyValueService.dropTable(TransactionConstants.TRANSACTION_TABLE);
    }

    public static void truncateTables(KeyValueService keyValueService) {
        keyValueService.truncateTable(TransactionConstants.TRANSACTION_TABLE);
    }
}
