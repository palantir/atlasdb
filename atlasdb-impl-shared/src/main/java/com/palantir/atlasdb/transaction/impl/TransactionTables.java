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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public final class TransactionTables {
    private TransactionTables() {
        // Utility class
    }

    // todo(gmaretic): consider creating the transaction table(s) dynamically as needed to avoid having an empty one
    public static void createTables(KeyValueService keyValueService) {
        keyValueService.createTables(ImmutableMap.of(
                TransactionConstants.TRANSACTION_TABLE,
                TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes(),
                TransactionConstants.TRANSACTIONS2_TABLE,
                TransactionConstants.TRANSACTIONS2_TABLE_METADATA.persistToBytes(),
                TransactionConstants.TRANSACTIONS3_TABLE,
                TransactionConstants.TRANSACTIONS3_TABLE_METADATA.persistToBytes()));
    }

    public static void truncateTables(KeyValueService keyValueService) {
        keyValueService.truncateTables(ImmutableSet.of(
                TransactionConstants.TRANSACTION_TABLE,
                TransactionConstants.TRANSACTIONS2_TABLE,
                TransactionConstants.TRANSACTIONS3_TABLE));
    }
}
