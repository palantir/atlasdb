/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BatchingLeaderElectionService implements LeaderElectionService, Closeable {
    private final LeaderElectionService delegate;
    private final DisruptorAutobatcher<Void, LeadershipToken> batcher;

    public BatchingLeaderElectionService(LeaderElectionService delegate) {
        this.delegate = delegate;
        this.batcher = Autobatchers.independent(this::processBatch)
                .safeLoggablePurpose("leader-election-service")
                .build();
    }

    @Override
    public void markNotEligibleForLeadership() {
        delegate.markNotEligibleForLeadership();
    }

    @Override
    public LeadershipToken blockOnBecomingLeader() throws InterruptedException {
        try {
            return batcher.apply(null).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    @Override
    public Optional<LeadershipToken> getCurrentTokenIfLeading() {
        return delegate.getCurrentTokenIfLeading();
    }

    @Override
    public ListenableFuture<StillLeadingStatus> isStillLeading(LeadershipToken token) {
        return delegate.isStillLeading(token);
    }

    @Override
    public boolean stepDown() {
        return delegate.stepDown();
    }

    @Override
    public boolean hostileTakeover() {
        return delegate.hostileTakeover();
    }

    @Override
    public Optional<HostAndPort> getRecentlyPingedLeaderHost() {
        return delegate.getRecentlyPingedLeaderHost();
    }

    private void processBatch(List<BatchElement<Void, LeadershipToken>> batch) {
        try {
            LeaderElectionService.LeadershipToken leadershipToken = delegate.blockOnBecomingLeader();
            batch.forEach(request -> request.result().set(leadershipToken));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            batch.forEach(request -> request.result().setException(e));
        }
    }

    @Override
    public void close() {
        batcher.close();
    }
}
