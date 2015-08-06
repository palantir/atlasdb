package com.palantir.lock;

import java.util.Set;

import com.google.common.collect.ForwardingObject;

public abstract class ForwardingRemoteLockService extends ForwardingObject implements RemoteLockService {

    @Override
    protected abstract RemoteLockService delegate();

    public LockResponse lockAnonymously(LockRequest request) throws InterruptedException {
        return delegate().lockAnonymously(request);
    }

    public LockResponse lockWithClient(String client, LockRequest request)
            throws InterruptedException {
        return delegate().lockWithClient(client, request);
    }

    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return delegate().unlockSimple(token);
    }

    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate().refreshLockRefreshTokens(tokens);
    }

    public Long getMinLockedInVersionId(LockClient client) {
        return delegate().getMinLockedInVersionId(client);
    }

    public long currentTimeMillis() {
        return delegate().currentTimeMillis();
    }

    public void logCurrentState() {
        delegate().logCurrentState();
    }

    public void logCurrentStateInconsistent() {
        delegate().logCurrentStateInconsistent();
    }

}
