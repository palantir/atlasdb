package com.palantir.atlasdb.transaction.service;

import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class KVSBasedTransactionService implements TransactionService {
    private final TransactionKVSWrapper kvsWrapper;

    public KVSBasedTransactionService(KeyValueService keyValueService) {
        this.kvsWrapper = new TransactionKVSWrapper(keyValueService);
    }

    @Override
    public Long get(long startTimestamp) {
        return kvsWrapper.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return kvsWrapper.get(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        kvsWrapper.putUnlessExists(startTimestamp, commitTimestamp);
    }
}
