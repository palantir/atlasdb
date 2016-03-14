/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.lock;

import java.math.BigInteger;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.common.annotation.Idempotent;

public abstract class ForwardingLockService extends ForwardingObject implements LockService {

    @Override
    protected abstract LockService delegate();

    @Override
    public LockRefreshToken lock(String client, LockRequest request)
            throws InterruptedException {
        return delegate().lock(client, request);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        return delegate().lockAndGetHeldLocks(client, request);
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return delegate().lockWithFullLockResponse(client, request);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return delegate().unlockSimple(SimpleHeldLocksToken.fromHeldLocksToken(token));
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate().unlockSimple(SimpleHeldLocksToken.fromLockRefreshToken(token));
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return delegate().unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return delegate().unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return delegate().getTokens(client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        Set<LockRefreshToken> refreshTokens = ImmutableSet.copyOf(
                Iterables.transform(tokens, HeldLocksTokens.getRefreshTokenFun()));
        Set<LockRefreshToken> goodTokens = delegate().refreshLockRefreshTokens(refreshTokens);
        Set<HeldLocksToken> ret = Sets.newHashSetWithExpectedSize(refreshTokens.size());
        Map<LockRefreshToken, HeldLocksToken> tokenMap = Maps.uniqueIndex(tokens, HeldLocksTokens.getRefreshTokenFun());
        for (LockRefreshToken goodToken : goodTokens) {
            HeldLocksToken lock = tokenMap.get(goodToken);
            ret.add(goodToken.refreshTokenWithExpriationDate(lock));
        }
        return ret;
    }

    @Override
    @Idempotent
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate().refreshLockRefreshTokens(tokens);
    }

    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return delegate().refreshGrant(grant);
    }

    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return delegate().refreshGrant(grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return delegate().convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return delegate().useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return delegate().useGrant(client, grantId);
    }

    @Override
    @Deprecated
    public Long getMinLockedInVersionId() {
        return delegate().getMinLockedInVersionId();
    }

    @Override
    public Long getMinLockedInVersionId(String client) {
        return delegate().getMinLockedInVersionId(client);
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return delegate().getMinLockedInVersionId(client);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return delegate().getLockServerOptions();
    }

    @Override
    public long currentTimeMillis() {
        return delegate().currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        delegate().logCurrentState();
    }
}
