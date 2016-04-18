package com.palantir.atlasdb.schema.stream.generated;

import java.util.Set;

import com.palantir.atlasdb.transaction.api.Transaction;

public class DeletingStreamStore {
    private final StreamTestStreamStore underlyingStreamStore;

    public DeletingStreamStore(StreamTestStreamStore underlyingStreamStore) {
        this.underlyingStreamStore = underlyingStreamStore;
    }

    public void deleteStreams(Transaction t, Set<Long> streamIds) {
        underlyingStreamStore.deleteStreams(t, streamIds);
    }
}
