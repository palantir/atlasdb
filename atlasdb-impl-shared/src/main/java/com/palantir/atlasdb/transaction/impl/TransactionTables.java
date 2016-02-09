package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class TransactionTables {
    public static void createTables(KeyValueService keyValueService) {
        keyValueService.createTable(TransactionConstants.TRANSACTION_TABLE, TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes());
    }

    public static void deleteTables(KeyValueService keyValueService) {
        keyValueService.dropTable(TransactionConstants.TRANSACTION_TABLE);
    }
}
