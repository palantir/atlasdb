/**
 * Copyright 2015 Palantir Technologies
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

import java.io.IOException;
import java.util.Collection;
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
import feign.Request.Options;
import feign.RequestTemplate;
import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.Target;

public class FailoverFeignTarget<T> implements Target<T>, Retryer {
    private static final Logger log = LoggerFactory.getLogger(FailoverFeignTarget.class);

    public static final int DEFAULT_MAX_BACKOFF_MILLIS = 3000;

    private static final double GOLDEN_RATIO = (Math.sqrt(5) + 1.0) / 2.0;

    private final ImmutableList<String> servers;
    private final Class<T> type;
    private final AtomicInteger failoverCount = new AtomicInteger();
    private final int failuresBeforeSwitching = 3;
    private final int numServersToTryBeforeFailing = 14;
    private final int fastFailoverTimeoutMillis = 10000;
    private final int maxBackoffMillis;
    private final RetrySemantics retrySemantics;

    private final AtomicLong failuresSinceLastSwitch = new AtomicLong();
    private final AtomicLong numSwitches = new AtomicLong();
    private final AtomicLong startTimeOfFastFailover = new AtomicLong();

    final ThreadLocal<Integer> mostRecentServerIndex = new ThreadLocal<>();

    public FailoverFeignTarget(Collection<String> servers, Class<T> type) {
        this(servers, DEFAULT_MAX_BACKOFF_MILLIS, type);
    }

    public FailoverFeignTarget(Collection<String> servers, int maxBackoffMillis, Class<T> type) {
        this(servers, maxBackoffMillis, type, RetrySemantics.getSemanticsFor(type));
    }

    @VisibleForTesting
    FailoverFeignTarget(
            Collection<String> servers,
            int maxBackoffMillis,
            Class<T> type,
            RetrySemantics retrySemantics) {
        Preconditions.checkArgument(maxBackoffMillis > 0);
        this.servers = ImmutableList.copyOf(ImmutableSet.copyOf(servers));
        this.type = type;
        this.maxBackoffMillis = maxBackoffMillis;
        this.retrySemantics = retrySemantics;
    }

    public void successfulCall() {
        numSwitches.set(0);
        failuresSinceLastSwitch.set(0);
        startTimeOfFastFailover.set(0);
    }

    @Override
    public void continueOrPropagate(RetryableException ex) {
        boolean isFastFailoverException = isFastFailoverException(ex);
        synchronized (this) {
            if (!isFastFailoverException && retrySemantics == RetrySemantics.NEVER_EXCEPT_ON_NON_LEADERS) {
                throw hardFailOwingToFailureOnLeader(ex);
            }
            // Only fail over if this failure was to the current server.
            // This means that no one on another thread has failed us over already.
            if (mostRecentServerIndex.get() != null && mostRecentServerIndex.get() == failoverCount.get()) {
                long failures = failuresSinceLastSwitch.incrementAndGet();
                if (isFastFailoverException || failures >= failuresBeforeSwitching) {
                    if (isFastFailoverException) {
                        // We did talk to a node successfully. It was shutting down but nodes are available
                        // so we shoudln't keep making the backoff higher.
                        numSwitches.set(0);
                        startTimeOfFastFailover.compareAndSet(0, System.currentTimeMillis());
                    } else {
                        numSwitches.incrementAndGet();
                        startTimeOfFastFailover.set(0);
                    }
                    failuresSinceLastSwitch.set(0);
                    failoverCount.incrementAndGet();
                }
            }
        }

        checkAndHandleFailure(ex);
        if (!isFastFailoverException) {
            pauseForBackOff(ex);
        }
    }

    private RuntimeException hardFailOwingToFailureOnLeader(RetryableException ex) {
        // Failure on a leader
        log.error("This connection has failed on a leader when its semantics instruct it not to retry"
                + " except on a non-leader.", ex);
        return ex == null ? new IllegalStateException("continueOrPropagate a null") : ex;
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
                    TimeUnit.MILLISECONDS.toSeconds(fastFailoverTimeoutMillis));
        } else if (failedDueToNumSwitches) {
            log.error("This connection has tried {} hosts rolling across {} servers, each {} times and has failed out.",
                    numServersToTryBeforeFailing, servers.size(), failuresBeforeSwitching, ex);
        }

        if (failedDueToFastFailover || failedDueToNumSwitches) {
            throw ex;
        }
    }

    @VisibleForTesting
    static boolean isFastFailoverException(RetryableException ex) {
        // If this is not-null, then we interpret this to mean that the server has thrown a 503 (so it might
        // not have been the leader).
        return ex.retryAfter() != null || ex instanceof PotentialFollowerException;
    }

    private void pauseForBackOff(RetryableException ex) {
        double pow = Math.pow(
                GOLDEN_RATIO,
                numSwitches.get() * failuresBeforeSwitching + failuresSinceLastSwitch.get());
        long timeout = Math.min(maxBackoffMillis, Math.round(pow));

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
        return new Client() {
            @Override
            public Response execute(Request request, Options options) throws IOException {
                Response response = client.execute(request, options);
                if (response.status() >= 200 && response.status() < 300) {
                    successfulCall();
                }
                return response;
            }
        };
    }
}
