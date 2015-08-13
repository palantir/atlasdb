package com.palantir.atlasdb.transaction.impl;

import com.palantir.lock.LockRefreshToken;

public class RawTransaction extends ForwardingTransaction {
    private final SnapshotTransaction delegate;
    private final LockRefreshToken lock;

    public RawTransaction(SnapshotTransaction delegate, LockRefreshToken lock) {
        this.delegate = delegate;
        this.lock = lock;
    }

    @Override
    public SnapshotTransaction delegate() {
        return delegate;
    }

    LockRefreshToken getImmutableTsLock() {
        return lock;
    }
}