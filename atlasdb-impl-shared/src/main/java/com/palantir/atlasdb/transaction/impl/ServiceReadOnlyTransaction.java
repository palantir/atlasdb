package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.AutoDelegate_Transaction;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = Transaction.class)
class ServiceReadOnlyTransaction implements AutoDelegate_Transaction {
    private final SnapshotTransaction delegate;

    ServiceReadOnlyTransaction(SnapshotTransaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }
}
