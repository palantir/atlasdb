/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.lock;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.lock.ForwardingRemoteLockService;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.logsafe.SafeArg;

public class AsyncUnlockingRemoteLockService extends ForwardingRemoteLockService {

    private static final Logger log = LoggerFactory.getLogger(AsyncUnlockingRemoteLockService.class);

    private final RemoteLockService delegate;
    // TODO(nziebart): we should replace the multi-threaded executor with a batch unlock endpoint
    // + single threaded executor.
    private final ExecutorService executor;

    public static AsyncUnlockingRemoteLockService synchronousWrapper(RemoteLockService delegate) {
        return new AsyncUnlockingRemoteLockService(delegate, MoreExecutors.newDirectExecutorService());
    }

    public AsyncUnlockingRemoteLockService(RemoteLockService delegate, ExecutorService executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @Override
    protected RemoteLockService delegate() {
        return delegate;
    }

    public void tryUnlockAsync(LockRefreshToken token) {
        executor.execute(() -> unlockOrLogFailure(token));
    }

    private void unlockOrLogFailure(LockRefreshToken token) {
        try {
            boolean wasUnlocked = delegate.unlock(token);
            if (!wasUnlocked) {
                log.info("Lock {} was expired at the time of unlocking", SafeArg.of("token", token));
            }
        } catch (Throwable t) {
            log.warn("Unable to unlock {}", SafeArg.of("token", token), t);
        }
    }

    public void tryRefreshLockRefreshTokensAsync(Iterable<LockRefreshToken> tokens,
            Consumer<Set<LockRefreshToken>> successHandler) {
        executor.execute(() -> refreshOrLogFailure(tokens, successHandler));
    }

    private void refreshOrLogFailure(Iterable<LockRefreshToken> tokens,
            Consumer<Set<LockRefreshToken>> successHandler) {
        Set<LockRefreshToken> refreshedTokens;
        try {
            refreshedTokens = delegate.refreshLockRefreshTokens(tokens);
        } catch (Throwable t) {
            log.warn("Unable to refresh {}", SafeArg.of("tokens", tokens), t);
            return;
        }

        successHandler.accept(refreshedTokens);
    }

}

