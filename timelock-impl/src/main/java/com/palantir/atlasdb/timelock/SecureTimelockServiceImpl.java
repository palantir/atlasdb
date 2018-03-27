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

package com.palantir.atlasdb.timelock;

import java.io.IOException;
import java.util.Set;

import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;

public class SecureTimelockServiceImpl implements SecureTimelockService {

    private final AsyncTimelockService delegate;

    public SecureTimelockServiceImpl(AsyncTimelockService delegate) {this.delegate = delegate;}

    @Override
    public long getFreshTimestamp(AuthHeader authHeader) {
        // check the authHeader
        if (authHeader.equals(AuthHeader.valueOf("foo"))) {
            return delegate.getFreshTimestamp();
        }
        throw new NotAuthorizedException("not authorized token");
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return delegate.unlock(tokens);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return delegate.refreshLockLeases(tokens);
    }

    @Override
    public AsyncResult<Void> waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public AsyncResult<LockToken> lock(LockRequest request) {
        return delegate.lock(request);
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        return delegate.lockImmutableTimestamp(request);
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    @POST
    @Path("fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    public long getFreshTimestamp() {
        return delegate.getFreshTimestamp();
    }

    @Override
    @POST
    @Path("fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    @POST
    @Path("fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    public void fastForwardTimestamp(long currentTimestamp) {
        delegate.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    @GET
    @Path("ping")
    @Produces(MediaType.TEXT_PLAIN)
    @CheckReturnValue(when = When.NEVER)
    public String ping() {
        return delegate.ping();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }


}
