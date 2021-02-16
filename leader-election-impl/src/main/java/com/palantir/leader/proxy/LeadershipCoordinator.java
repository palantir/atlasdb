/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.net.HostAndPort;
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

/**
 * This is the component of TimeLock that is responsible for coordinating and maintaining a consistent leadership state
 * in the cluster. Each node keeps trying to gain leadership upon a) startup and b) loss of leadership, see
 * {@link #tryToGainLeadership()}
 *
 * {@link LeadershipCoordinator} is finally used by {@link AwaitingLeadershipProxy} instances to check if this node
 *  can service requests or not.
 *
 * {@link LeadershipCoordinator} relies on {@link LeaderElectionService} for the following -
 * 1. To get leadership token if we are already leading
 * 2. To block on becoming the leader if we not the leader anymore
 * 3. To verify if we are still the leader
 */
public final class LeadershipCoordinator implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(LeadershipCoordinator.class);

    private static final Duration GAIN_LEADERSHIP_BACKOFF = Duration.ofMillis(500);

    private final LeaderElectionService leaderElectionService;
    private final ExecutorService executor;
    /**
     * This is used as the handoff point between the executor doing the blocking and the invocation calls.  It is set by
     * the executor after the delegateRef is set. It is cleared out by invoke which will close the delegate and spawn a
     * new blocking task.
     */
    private final AtomicReference<LeadershipToken> leadershipTokenRef;

    private volatile boolean isClosed;

    private LeadershipCoordinator(LeaderElectionService leaderElectionService) {
        this.leaderElectionService = leaderElectionService;
        this.executor = PTExecutors.newSingleThreadExecutor();
        this.leadershipTokenRef = new AtomicReference<>();
        this.isClosed = false;
    }

    public static LeadershipCoordinator create(LeaderElectionService leaderElectionService) {
        LeadershipCoordinator leadershipCoordinator = new LeadershipCoordinator(leaderElectionService);
        leadershipCoordinator.tryToGainLeadership();
        return leadershipCoordinator;
    }

    ListenableFuture<StillLeadingStatus> isStillLeading(LeadershipToken leadershipToken) {
        return leaderElectionService.isStillLeading(leadershipToken);
    }

    void markAsNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        if (leadershipTokenRef.compareAndSet(leadershipToken, null)) {
            log.warn("Lost leadership", cause);
            tryToGainLeadership();
        }
    }

    LeadershipToken getLeadershipToken() {
        LeadershipToken leadershipToken = leadershipTokenRef.get();

        if (leadershipToken == null) {
            Optional<HostAndPort> maybeLeader = leaderElectionService.getRecentlyPingedLeaderHost();

            if (maybeLeader.isPresent()) {
                // There's a chance that we can gain leadership while generating this exception.
                // In this case, we should be able to get a leadership token after all
                leadershipToken = leadershipTokenRef.get();
            }

            if (leadershipToken == null) {
                // If leadershipToken is still null, then someone's the leader, but it isn't us.
                throw notCurrentLeaderException(maybeLeader, "method invoked on a non-leader", null /* cause */);
            }
        }

        return leadershipToken;
    }

    boolean isStillCurrentToken(LeadershipToken leadershipToken) {
        return leadershipToken == leadershipTokenRef.get();
    }

    NotCurrentLeaderException notCurrentLeaderException(String message) {
        return notCurrentLeaderException(message, null /* cause */);
    }

    NotCurrentLeaderException notCurrentLeaderException(String message, @Nullable Throwable cause) {
        return notCurrentLeaderException(leaderElectionService.getRecentlyPingedLeaderHost(), message, cause);
    }

    private NotCurrentLeaderException notCurrentLeaderException(
            Optional<HostAndPort> recentlyPingedLeaderHost, String message, @Nullable Throwable cause) {
        return recentlyPingedLeaderHost
                .map(hostAndPort -> new NotCurrentLeaderException(message, cause, hostAndPort))
                .orElseGet(() -> new NotCurrentLeaderException(message, cause));
    }

    private void tryToGainLeadership() {
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
                throw new SafeIllegalStateException(
                        "Failed to submit task to gain leadership but coordinator not closed", e);
            }
        }
    }

    private void gainLeadershipWithRetry() {
        while (!gainLeadershipBlocking()) {
            try {
                Thread.sleep(GAIN_LEADERSHIP_BACKOFF.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Gain leadership backoff interrupted");
                if (isClosed) {
                    log.info("Gain leadership with retry terminated as the coordinator is closed");
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
            log.warn("Attempt to gain leadership interrupted", e);
        } catch (Throwable e) {
            log.error("Problem blocking on leadership", e);
        }
        return false;
    }

    private void onGainedLeadership(LeadershipToken leadershipToken) {
        log.debug("Gained leadership!");

        if (isClosed) {
            return;
        } else {
            leadershipTokenRef.set(leadershipToken);
            log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
        }
    }

    @Override
    public void close() {
        log.debug("Closing leadership coordinator.");
        isClosed = true;
        executor.shutdownNow();
    }
}
