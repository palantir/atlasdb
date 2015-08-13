package com.palantir.lock;

import java.util.Set;

import com.google.common.collect.ForwardingObject;

public abstract class ForwardingRemoteLockService extends ForwardingObject implements RemoteLockService {

    @Override
    protected abstract RemoteLockService delegate();

    @Override
    public LockRefreshToken lockAnonymously(LockRequest request) throws InterruptedException {
        return delegate().lockAnonymously(request);
    }

    @Override
    public LockRefreshToken lockWithClient(String client, LockRequest request)
            throws InterruptedException {
        return delegate().lockWithClient(client, request);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate().unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate().refreshLockRefreshTokens(tokens);
    }

    @Override
    public Long getMinLockedInVersionId(String client) {
        return delegate().getMinLockedInVersionId(client);
    }

    @Override
    public long currentTimeMillis() {
        return delegate().currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        delegate().logCurrentState();
    }

    @Override
    public void logCurrentStateInconsistent() {
        delegate().logCurrentStateInconsistent();
    }

}
