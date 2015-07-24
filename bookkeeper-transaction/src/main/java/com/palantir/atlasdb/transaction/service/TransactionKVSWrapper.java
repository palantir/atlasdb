package com.palantir.atlasdb.transaction.service;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class TransactionKVSWrapper {
    // The maximum key-value store timestamp (exclusive) at which data is stored in transaction table.
    // All entries in transaction table are stored with timestamp 0
    private static final long MAX_TIMESTAMP = 1L;

    private final KeyValueService keyValueService;

    public TransactionKVSWrapper(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    private static Cell getTransactionCell(long startTimestamp) {
        return Cell.create(TransactionConstants.getValueForTimestamp(startTimestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
    }

    public Long get(Long startTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        Map<Cell, Value> returnMap = keyValueService.get(TransactionConstants.TRANSACTION_TABLE,
                                                         ImmutableMap.of(cell, MAX_TIMESTAMP));
        if (returnMap.containsKey(cell))
            return TransactionConstants.getTimestampForValue(returnMap.get(cell).getContents());
        else
            return null;
    }

    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        Map<Long, Long> result = Maps.newHashMap();
        Map<Cell, Long> startTsMap = Maps.newHashMap();
        for (Long startTimestamp: startTimestamps) {
            Cell k = getTransactionCell(startTimestamp);
            startTsMap.put(k, MAX_TIMESTAMP);
        }

        Map<Cell, Value> rawResults = keyValueService.get(TransactionConstants.TRANSACTION_TABLE, startTsMap);
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = TransactionConstants.getTimestampForValue(e.getKey().getRowName());
            long commitTs = TransactionConstants.getTimestampForValue(e.getValue().getContents());
            result.put(startTs, commitTs);
        }

        return result;
    }

    // It works only if key-value store supports putUnlessExists.
    public void putUnlessExists(Long startTimestamp, Long commitTimestamp) {
        Cell key = getTransactionCell(startTimestamp);
        byte[] value = TransactionConstants.getValueForTimestamp(commitTimestamp);
        keyValueService.putUnlessExists(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(key, value));
    }

    public void putAll(Map<Long, Long> timestampMap) {
        Map<Cell, byte[]> kvMap = new HashMap<Cell, byte[]> ();
        for (Map.Entry<Long, Long> entry: timestampMap.entrySet()) {
            kvMap.put(
                    getTransactionCell(entry.getKey()),
                    TransactionConstants.getValueForTimestamp(entry.getValue()));
        }

        keyValueService.put(TransactionConstants.TRANSACTION_TABLE, kvMap, 0); // This can throw unchecked exceptions
    }

    // The log has to be closed
    public void flushLog(WriteAheadLog log) {
        Map<Long, Long> map = new HashMap<Long, Long>();
        for (TransactionLogEntry entry: log) {
            map.put(entry.getStartTimestamp(), entry.getCommitTimestamp());
        }
        putAll(map);
    }

}
