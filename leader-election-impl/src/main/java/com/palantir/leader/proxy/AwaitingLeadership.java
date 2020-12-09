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
package com.palantir.leader.proxy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwaitingLeadership implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadership.class);

    private static final Duration GAIN_LEADERSHIP_BACKOFF = Duration.ofMillis(500);

    private final LeaderElectionService leaderElectionService;
    private final ExecutorService executor;
    /**
     * This is used as the handoff point between the executor doing the blocking
     * and the invocation calls.  It is set by the executor after the delegateRef is set.
     * It is cleared out by invoke which will close the delegate and spawn a new blocking task.
     */
    private final AtomicReference<LeadershipToken> leadershipTokenRef;

    private volatile boolean isClosed;

    private AwaitingLeadership(LeaderElectionService leaderElectionService) {
        this.leaderElectionService = leaderElectionService;
        this.executor = PTExecutors.newSingleThreadExecutor();
        this.leadershipTokenRef = new AtomicReference<>();
        this.isClosed = false;
    }

    public static AwaitingLeadership create(LeaderElectionService leaderElectionService) {
        AwaitingLeadership awaitingLeadership = new AwaitingLeadership(leaderElectionService);
        // awaitingLeadership.tryToGainLeadership();
        return awaitingLeadership;
    }

    public void markAsNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        log.warn("Lost leadership", cause);
        if (leadershipTokenRef.compareAndSet(leadershipToken, null)) {
            tryToGainLeadership();
        }
    }

    public ListenableFuture<StillLeadingStatus> getStillLeading(LeadershipToken leadershipToken) {
        return leaderElectionService.isStillLeading(leadershipToken);
    }

    @Override
    // Have to register close
    public void close() {
        log.debug("Closing leadership proxy");
        isClosed = true;
        executor.shutdownNow();
    }

    void tryToGainLeadership() {
        Optional<LeadershipToken> currentToken = leaderElectionService.getCurrentTokenIfLeading();
        if (currentToken.isPresent()) {
            onGainedLeadership(currentToken.get());
        } else {
            tryToGainLeadershipAsync();
        }
    }

    private void tryToGainLeadershipAsync() {
        try {
            executor.execute(this::gainLeadershipWithRetry);
        } catch (RejectedExecutionException e) {
            if (!isClosed) {
                throw new SafeIllegalStateException("failed to submit task but proxy not closed", e);
            }
        }
    }

    private void gainLeadershipWithRetry() {
        while (!gainLeadershipBlocking()) {
            try {
                Thread.sleep(GAIN_LEADERSHIP_BACKOFF.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("gain leadership backoff interrupted");
                if (isClosed) {
                    log.info("gain leadership with retry terminated as the proxy is closed");
                    return;
                }
            }
        }
    }

    private boolean gainLeadershipBlocking() {
        log.debug("Block until gained leadership");
        try {
            LeadershipToken leadershipToken = leaderElectionService.blockOnBecomingLeader();
            onGainedLeadership(leadershipToken);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("attempt to gain leadership interrupted", e);
        } catch (Throwable e) {
            log.error("problem blocking on leadership", e);
        }
        return false;
    }

    private void onGainedLeadership(LeadershipToken leadershipToken) {
        log.debug("Gained leadership, getting delegate to start serving calls");

        if (isClosed) {
            return;
        } else {
            updateLeadershipTokenIfNewToken(leadershipToken);
            log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
        }
    }

    @VisibleForTesting
    LeadershipToken getLeadershipToken() {
        LeadershipToken leadershipToken = leadershipTokenRef.get();

        if (leadershipToken == null) {
            NotCurrentLeaderException notCurrentLeaderException =
                    notCurrentLeaderException("method invoked on a non-leader");

            if (notCurrentLeaderException.getServiceHint().isPresent()) {
                // There's a chance that we can gain leadership while generating this exception.
                // In this case, we should be able to get a leadership token after all
                leadershipToken = leadershipTokenRef.get();
                // If leadershipToken is still null, then someone's the leader, but it isn't us.
            }

            if (leadershipToken == null) {
                throw notCurrentLeaderException;
            }
        }

        return leadershipToken;
    }

    boolean isStillCurrentToken(LeadershipToken leadershipToken) {
        return leadershipToken == leadershipTokenRef.get();
    }

    private void updateLeadershipTokenIfNewToken(LeadershipToken leadershipToken) {
        LeadershipToken currentLeadershipToken = leadershipTokenRef.get();

        if (leadershipToken == currentLeadershipToken || currentLeadershipToken.sameAs(leadershipToken)) {
            return;
        }

        leadershipTokenRef.set(leadershipToken);
    }

    NotCurrentLeaderException notCurrentLeaderException(String message, @Nullable Throwable cause) {
        return leaderElectionService
                .getRecentlyPingedLeaderHost()
                .map(hostAndPort -> new NotCurrentLeaderException(message, cause, hostAndPort))
                .orElseGet(() -> new NotCurrentLeaderException(message, cause));
    }

    NotCurrentLeaderException notCurrentLeaderException(String message) {
        return notCurrentLeaderException(message, null /* cause */);
    }
}
