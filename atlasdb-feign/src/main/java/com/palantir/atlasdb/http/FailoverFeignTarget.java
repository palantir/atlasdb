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
package com.palantir.atlasdb.http;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.time.Clock;
import com.palantir.logsafe.SafeArg;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import feign.Client;
import feign.Request;
import feign.RequestTemplate;
import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.Target;

public class FailoverFeignTarget<T> implements Target<T>, Retryer {
    private static final Logger log = LoggerFactory.getLogger(FailoverFeignTarget.class);

    public static final Duration DEFAULT_MAX_BACKOFF = Duration.ofMillis(1000);
    public static final Duration MAX_BACKOFF_BEFORE_ROUND_ROBIN_RETRY = Duration.ofMillis(250);

    private static final double GOLDEN_RATIO = (Math.sqrt(5) + 1.0) / 2.0;

    private final ImmutableList<String> servers;
    private final Class<T> type;
    private final AtomicInteger failoverCount = new AtomicInteger();
    @VisibleForTesting
    final int failuresBeforeSwitching = 3;
    private final int numServersToTryBeforeFailing = 14;
    private final int fastFailoverTimeoutMillis = 10000;
    private final long maxBackoffMillis;

    private final AtomicLong failuresSinceLastSwitch = new AtomicLong();
    private final AtomicLong numSwitches = new AtomicLong();
    private final AtomicReference<Optional<FastFailoverState>> fastFailoverState
            = new AtomicReference<>(Optional.empty());

    private final ThreadLocal<Integer> mostRecentServerIndex = new ThreadLocal<>();

    private final Clock clock;

    public FailoverFeignTarget(Collection<String> servers, long maxBackoffMillis, Class<T> type) {
        this(servers, maxBackoffMillis, type, System::currentTimeMillis);
    }

    @VisibleForTesting
    FailoverFeignTarget(Collection<String> servers, long maxBackoffMillis, Class<T> type, Clock clock) {
        Preconditions.checkArgument(maxBackoffMillis > 0);
        this.servers = ImmutableList.copyOf(ImmutableSet.copyOf(servers));
        this.type = type;
        this.maxBackoffMillis = maxBackoffMillis;
        this.clock = clock;
    }

    private void successfulCall() {
        numSwitches.set(0);
        failuresSinceLastSwitch.set(0);
        fastFailoverState.set(Optional.empty());
    }

    @Override
    public void continueOrPropagate(RetryableException ex) {
        ExceptionRetryBehaviour retryBehaviour = ExceptionRetryBehaviour.getRetryBehaviourForException(ex);

        synchronized (this) {
            // Only fail over if this failure was to the current server.
            // This means that no one on another thread has failed us over already.
            if (mostRecentServerIndex.get() != null && mostRecentServerIndex.get() == failoverCount.get()) {
                long failures = failuresSinceLastSwitch.incrementAndGet();
                if (shouldSwitchNode(retryBehaviour, failures)) {
                    failoverToNextNode(retryBehaviour);
                } else if (retryBehaviour.shouldRetryInfinitelyManyTimes()) {
                    failuresSinceLastSwitch.set(0);
                }
            }
        }

        checkAndHandleFailure(ex);
        if (retryBehaviour.shouldBackoffAndTryOtherNodes()) {
            int numFailovers = failoverCount.get();
            if (numFailovers > 0 && numFailovers % servers.size() == 0) {

                // We implement some randomness around the expected value of BACKOFF_BEFORE_ROUND_ROBIN_RETRY_MILLIS.
                // Even though this is not exponential backoff, should be enough to avoid a thundering herd problem.
                long pauseTimeWithJitter = ThreadLocalRandom.current()
                        .nextLong(MAX_BACKOFF_BEFORE_ROUND_ROBIN_RETRY.toMillis());

                pauseForBackoff(ex, pauseTimeWithJitter);
            }
        } else {
            pauseForBackoff(ex);
        }
    }

    private boolean shouldSwitchNode(ExceptionRetryBehaviour retryBehaviour, long failures) {
        return retryBehaviour.shouldBackoffAndTryOtherNodes()
                || (!retryBehaviour.shouldRetryInfinitelyManyTimes() && failures >= failuresBeforeSwitching);
    }

    private void failoverToNextNode(ExceptionRetryBehaviour retryBehaviour) {
        if (retryBehaviour.shouldBackoffAndTryOtherNodes()) {
            // We did talk to a node successfully. It was shutting down but nodes are available
            // so we shouldn't keep making the backoff higher.
            numSwitches.set(0);
            fastFailoverState.updateAndGet(stateOptional ->
                    Optional.of(stateOptional.map(FastFailoverState::withAdditionalFailedServer)
                            .orElseGet(() -> FastFailoverState.of(clock.getTimeMillis(), 1))));
        } else {
            numSwitches.incrementAndGet();
            fastFailoverState.set(Optional.empty());
        }
        failuresSinceLastSwitch.set(0);
        failoverCount.incrementAndGet();
    }

    private void checkAndHandleFailure(RetryableException ex) {
        boolean failedDueToFastFailover = hasFailedDueToFastFailover();
        boolean failedDueToNumSwitches = numSwitches.get() >= numServersToTryBeforeFailing;

        if (failedDueToFastFailover) {
            log.error("This connection has been instructed to fast failover for {}"
                    + " seconds without establishing a successful connection, and connected to all remote hosts ({})."
                    + " The remote hosts have been in a fast failover state for too long.",
                    SafeArg.of("duration", TimeUnit.MILLISECONDS.toSeconds(fastFailoverTimeoutMillis)),
                    SafeArg.of("remoteHosts", servers),
                    ex);
        } else if (failedDueToNumSwitches) {
            log.error("This connection has tried {} hosts rolling across {} servers, each {} times and has failed out.",
                    numServersToTryBeforeFailing, servers.size(), failuresBeforeSwitching, ex);
        }

        if (failedDueToFastFailover || failedDueToNumSwitches) {
            throw ex;
        }
    }

    /**
     * We say that a {@link FailoverFeignTarget} has failed due to fast failover if two conditions are satisfied:
     *
     * <ol>
     *     <li>
     *         The cluster has only received fast failover exceptions from remote hosts for the last
     *         {@link FailoverFeignTarget#fastFailoverTimeoutMillis} milliseconds.
     *     </li>
     *     <li>
     *         All nodes have been tried at least once.
     *     </li>
     * </ol>
     *
     * @return whether this target has failed due to fast failover
     */
    private boolean hasFailedDueToFastFailover() {
        return fastFailoverState.get()
                .map(state -> hasBeenInFastFailoverStateForTooLong(state) && hasTriedAllNodesInFastFailover(state))
                .orElse(false);
    }

    private boolean hasBeenInFastFailoverStateForTooLong(FastFailoverState failoverState) {
        return clock.getTimeMillis() - failoverState.startTime() > fastFailoverTimeoutMillis;
    }

    private boolean hasTriedAllNodesInFastFailover(FastFailoverState failoverState) {
        return failoverState.numberOfServersTried() >= servers.size();
    }

    private void pauseForBackoff(RetryableException ex) {
        double exponentialPauseTime = Math.pow(
                GOLDEN_RATIO,
                numSwitches.get() * failuresBeforeSwitching + failuresSinceLastSwitch.get());
        long cappedPauseTime = Math.min(maxBackoffMillis, Math.round(exponentialPauseTime));

        // We use the Full Jitter (https://www.awsarchitectureblog.com/2015/03/backoff.html).
        // We prioritize a low server load over completion time.
        long pauseTimeWithJitter = ThreadLocalRandom.current().nextLong(cappedPauseTime);

        pauseForBackoff(ex, pauseTimeWithJitter);
    }

    @VisibleForTesting
    void pauseForBackoff(RetryableException ex, long pauseTime) {
        long timeout = Math.min(maxBackoffMillis, pauseTime);

        try {
            log.trace("Pausing {}ms before retrying", timeout);
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ex;
        }
    }

    @SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    @Override
    public Retryer clone() {
        mostRecentServerIndex.remove();
        return this;
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public String name() {
        return "server list: " + servers;
    }

    @Override
    public String url() {
        int indexToHit = failoverCount.get();
        mostRecentServerIndex.set(indexToHit);
        return servers.get(indexToHit % servers.size());
    }

    @Override
    public Request apply(RequestTemplate input) {
        if (input.url().indexOf("http") != 0) {
            input.insert(0, url());
        }
        return input.request();
    }

    public Client wrapClient(final Client client)  {
        return (request, options) -> {
            try (Response response = client.execute(request, options)) {
                if (response.status() >= 200 && response.status() < 300) {
                    successfulCall();
                }
                return response;
            }
        };
    }

    @Value.Immutable
    interface FastFailoverState {
        @Value.Parameter
        long startTime();
        @Value.Parameter
        long numberOfServersTried();

        default FastFailoverState withAdditionalFailedServer() {
            return FastFailoverState.of(startTime(), numberOfServersTried() + 1);
        }

        static FastFailoverState of(long startTime, long numberOfServersTried) {
            return ImmutableFastFailoverState.of(startTime, numberOfServersTried);
        }
    }
}
