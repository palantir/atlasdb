/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.v2.AutoDelegate_TimelockService;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.processors.AutoDelegate;

/**
 * This class serves as a bridge to allow newer Atlas clients to maintain compatibility with TimeLock servers that
 * do not support the {@link TimelockService::startAtlasDbTransaction()} operation.
 *
 * Initially, we assume that the server does support this operation. If we get a 404, we assume that the server does
 * not support this. We try the new endpoints once an hour in case someone upgraded TimeLock (that said, if a user
 * wants to see the perf gain more quickly, a rolling bounce works).
 *
 * There is a risk of a race condition in terms of multiple threads updating serverSupportsStartAtlasDbTransaction to
 * different values. This only arises if starting a transaction takes in excess of the period of the rate limiter
 * (one hour by default), so we believe it is unlikely. We also are not dependent on this for correctness, only for
 * performance.
 */
@AutoDelegate(typeToExtend = TimelockService.class)
public class ImmutableTimestampBridgingTimeLockService implements AutoDelegate_TimelockService {
    private static final double ONCE_PER_HOUR = 1. / TimeUnit.HOURS.toSeconds(1);

    private final TimelockService delegate;
    private final RateLimiter rateLimiter;

    private volatile boolean serverSupportsStartAtlasDbTransaction = true;

    @VisibleForTesting
    ImmutableTimestampBridgingTimeLockService(TimelockService delegate, RateLimiter rateLimiter) {
        this.delegate = delegate;
        this.rateLimiter = rateLimiter;
    }

    public static TimelockService create(TimelockService delegate) {
        return new ImmutableTimestampBridgingTimeLockService(delegate, RateLimiter.create(ONCE_PER_HOUR));
    }

    @Override
    public TimelockService delegate() {
        return delegate;
    }

    @Override
    public StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request) {
        if (shouldUseStartAtlasDbTransactionEndpoint()) {
            try {
                StartAtlasDbTransactionResponse response = delegate.startAtlasDbTransaction(request);
                serverSupportsStartAtlasDbTransaction = true;
                return response;
            } catch (AtlasDbRemoteException remoteException) {
                if (remoteException.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                    // The server 404'd which almost certainly means it doesn't have our new APIs yet
                    serverSupportsStartAtlasDbTransaction = false;
                    return startTransactionViaConstituentCalls(request);
                }

                // The remote exception was for some other reason
                throw remoteException;
            }
        } else {
            // We don't believe the server supports the operation, and we don't want to try again just yet.
            return startTransactionViaConstituentCalls(request);
        }
    }

    private boolean shouldUseStartAtlasDbTransactionEndpoint() {
        return serverSupportsStartAtlasDbTransaction || rateLimiter.tryAcquire();
    }

    private StartAtlasDbTransactionResponse startTransactionViaConstituentCalls(IdentifiedTimeLockRequest request) {
        return StartAtlasDbTransactionResponse.of(
                delegate.lockImmutableTimestamp(request),
                delegate.getFreshTimestamp());
    }
}
