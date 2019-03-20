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
package com.palantir.leader.proxy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;

public final class AwaitingLeadershipService {

    private static final Logger log = LoggerFactory.getLogger(AwaitingLeadershipService.class);
    private static final Duration GAIN_LEADERSHIP_BACKOFF = Duration.ofMillis(500);

    private List<Consumer<LeadershipToken>> onGained = new ArrayList<>();
    private List<BiConsumer<LeadershipToken, Throwable>> onLost = new ArrayList<>();

    final LeaderElectionService leaderElectionService;
    final ExecutorService executor;
    /**
     * This is used as the handoff point between the executor doing the blocking
     * and the invocation calls.  It is set by the executor after the delegateRef is set.
     * It is cleared out by invoke which will close the delegate and spawn a new blocking task.
     */
    final AtomicReference<LeadershipToken> leadershipTokenRef = new AtomicReference<>();

    public static AwaitingLeadershipService create(LeaderElectionService delegate) {
        AwaitingLeadershipService instance = new AwaitingLeadershipService(delegate);
        instance.tryToGainLeadership();
        return instance;
    }

    private AwaitingLeadershipService(LeaderElectionService leaderElectionService) {
        this.leaderElectionService = leaderElectionService;
        this.executor = PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true));
    }

    private void tryToGainLeadership() {
        Optional<LeadershipToken> currentToken = leaderElectionService.getCurrentTokenIfLeading();
        if (currentToken.isPresent()) {
            onGainedLeadership(currentToken.get());
        } else {
            tryToGainLeadershipAsync();
        }
    }

    private synchronized void onGainedLeadership(LeadershipToken leadershipToken)  {
        log.debug("Gained leadership, getting delegates to start serving calls");
        // We are now the leader, creating delegates; the ordering is important to maintain correctness
        onGained.forEach(action -> action.accept(leadershipToken));
        leadershipTokenRef.set(leadershipToken);
        log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
    }

    private void tryToGainLeadershipAsync() {
        executor.submit(this::gainLeadershipWithRetry);
    }

    private void gainLeadershipWithRetry() {
        while (!gainLeadershipBlocking()) {
            try {
                Thread.sleep(GAIN_LEADERSHIP_BACKOFF.toMillis());
            } catch (InterruptedException e) {
                log.warn("gain leadership backoff interrupted");
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
            log.warn("attempt to gain leadership interrupted", e);
        } catch (Throwable e) {
            log.error("problem blocking on leadership", e);
        }
        return false;
    }

    @VisibleForTesting
    LeadershipToken getLeadershipToken() {
        LeadershipToken leadershipToken = leadershipTokenRef.get();

        if (leadershipToken == null) {
            NotCurrentLeaderException notCurrentLeaderException = notCurrentLeaderException(
                    "method invoked on a non-leader");

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

    private NotCurrentLeaderException notCurrentLeaderException(String message, @Nullable Throwable cause) {
        Optional<HostAndPort> maybeLeader = leaderElectionService.getSuspectedLeaderInMemory();
        if (maybeLeader.isPresent()) {
            HostAndPort leaderHint = maybeLeader.get();
            return new NotCurrentLeaderException(message + "; hinting suspected leader host " + leaderHint,
                    cause, leaderHint);
        } else {
            return new NotCurrentLeaderException(message, cause);
        }
    }

    private NotCurrentLeaderException notCurrentLeaderException(String message) {
        return notCurrentLeaderException(message, null /* cause */);
    }

    public synchronized void markAsNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        log.warn("Lost leadership", cause);
        if (leadershipTokenRef.compareAndSet(leadershipToken, null)) {
            onLost.forEach(consumer -> consumer.accept(leadershipToken, cause));
            tryToGainLeadership();
        }
        throw notCurrentLeaderException("method invoked on a non-leader (leadership lost)", cause);
    }

    public synchronized void registerLeadershipTasks(Consumer<LeadershipToken> onGainedLeadership,
            BiConsumer<LeadershipToken, Throwable> markAsNotLeading) {
        LeadershipToken currentToken = leadershipTokenRef.get();
        if (currentToken != null) {
            onGainedLeadership.accept(currentToken);
        }
        onGained.add(onGainedLeadership);
        onLost.add(markAsNotLeading);
    }

    public boolean isStillCurrentToken(LeadershipToken leadershipToken) {
        return leadershipTokenRef.get() == leadershipToken;
    }

    public StillLeadingStatus isStillLeading(LeadershipToken leadershipToken) {
        return leaderElectionService.isStillLeading(leadershipToken);
    }

    public Optional<HostAndPort> getSuspectedLeaderInMemory() {
        return leaderElectionService.getSuspectedLeaderInMemory();
    }
}
