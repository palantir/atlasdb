/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.http;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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

    public static final int DEFAULT_MAX_BACKOFF_MILLIS = 3000;
    public static final long BACKOFF_BEFORE_ROUND_ROBIN_RETRY_MILLIS = 500L;

    private static final double GOLDEN_RATIO = (Math.sqrt(5) + 1.0) / 2.0;

    private final ImmutableList<String> servers;
    private final Class<T> type;
    private final AtomicInteger failoverCount = new AtomicInteger();
    @VisibleForTesting
    final int failuresBeforeSwitching = 3;
    private final int numServersToTryBeforeFailing = 14;
    private final int fastFailoverTimeoutMillis = 10000;
    private final int maxBackoffMillis;

    private final AtomicLong failuresSinceLastSwitch = new AtomicLong();
    private final AtomicLong numSwitches = new AtomicLong();
    private final AtomicLong startTimeOfFastFailover = new AtomicLong();

    private final ThreadLocal<Integer> mostRecentServerIndex = new ThreadLocal<>();

    public FailoverFeignTarget(Collection<String> servers, Class<T> type) {
        this(servers, DEFAULT_MAX_BACKOFF_MILLIS, type);
    }

    public FailoverFeignTarget(Collection<String> servers, int maxBackoffMillis, Class<T> type) {
        Preconditions.checkArgument(maxBackoffMillis > 0);
        this.servers = ImmutableList.copyOf(ImmutableSet.copyOf(servers));
        this.type = type;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    public void sucessfulCall() {
        numSwitches.set(0);
        failuresSinceLastSwitch.set(0);
        startTimeOfFastFailover.set(0);
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
                        .nextLong(BACKOFF_BEFORE_ROUND_ROBIN_RETRY_MILLIS / 2,
                                (BACKOFF_BEFORE_ROUND_ROBIN_RETRY_MILLIS * 3) / 2);

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
            startTimeOfFastFailover.compareAndSet(0, System.currentTimeMillis());
        } else {
            numSwitches.incrementAndGet();
            startTimeOfFastFailover.set(0);
        }
        failuresSinceLastSwitch.set(0);
        failoverCount.incrementAndGet();
    }

    private void checkAndHandleFailure(RetryableException ex) {
        final long fastFailoverStartTime = startTimeOfFastFailover.get();
        final long currentTime = System.currentTimeMillis();
        boolean failedDueToFastFailover = fastFailoverStartTime != 0
                && (currentTime - fastFailoverStartTime) > fastFailoverTimeoutMillis;
        boolean failedDueToNumSwitches = numSwitches.get() >= numServersToTryBeforeFailing;

        if (failedDueToFastFailover) {
            log.error("This connection has been instructed to fast failover for {}"
                    + " seconds without establishing a successful connection."
                    + " The remote hosts have been in a fast failover state for too long.",
                    TimeUnit.MILLISECONDS.toSeconds(fastFailoverTimeoutMillis), ex);
        } else if (failedDueToNumSwitches) {
            log.error("This connection has tried {} hosts rolling across {} servers, each {} times and has failed out.",
                    numServersToTryBeforeFailing, servers.size(), failuresBeforeSwitching, ex);
        }

        if (failedDueToFastFailover || failedDueToNumSwitches) {
            throw ex;
        }
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
            Response response = client.execute(request, options);
            if (response.status() >= 200 && response.status() < 300) {
                sucessfulCall();
            }
            return response;
        };
    }
}
