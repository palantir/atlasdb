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
package com.palantir.lock;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public abstract class SimplifyingLockService implements AutoDelegate_LockService, CloseableLockService {

    @Override
    public boolean unlock(HeldLocksToken token) {
        return delegate().unlockSimple(SimpleHeldLocksToken.fromHeldLocksToken(token));
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate().unlockSimple(SimpleHeldLocksToken.fromLockRefreshToken(token));
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        Set<LockRefreshToken> refreshTokens =
                ImmutableSet.copyOf(Iterables.transform(tokens, HeldLocksTokens.getRefreshTokenFun()));
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
    public void close() {
        if (delegate() instanceof AutoCloseable) {
            try {
                ((AutoCloseable) delegate()).close();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
