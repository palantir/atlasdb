package com.palantir.atlasdb.client;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import feign.Client;
import feign.Request;
import feign.Request.Options;
import feign.RequestTemplate;
import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.Target;
import jersey.repackaged.com.google.common.collect.ImmutableSet;

public class FailoverFeignTarget<T> implements Target<T>, Retryer {
    private static final Logger log = LoggerFactory.getLogger(FailoverFeignTarget.class);

    private final ImmutableList<String> servers;
    private final Class<T> type;
    private final AtomicInteger failoverCount = new AtomicInteger();
    private final int failuresBeforeSwitching = 5;
    private final int numServersToTryBeforeFailing = 8;
    private final int fastFailoverTimeoutMillis = 10000;

    private final AtomicLong failuresSinceLastSwitch = new AtomicLong();
    private final AtomicLong numSwitches = new AtomicLong();
    private final AtomicLong startTimeOfFastFailover = new AtomicLong();

    final ThreadLocal<Integer> mostRecentServerIndex = new ThreadLocal<Integer>();

    public FailoverFeignTarget(Collection<String> servers, Class<T> type) {
        this.servers = ImmutableList.copyOf(ImmutableSet.copyOf(servers));
        this.type = type;
    }

    public void sucessfulCall() {
        numSwitches.set(0);
        failuresSinceLastSwitch.set(0);
        startTimeOfFastFailover.set(0);
    }

    @Override
    public void continueOrPropagate(RetryableException e) {

        boolean isFailoverException;
        if (e.retryAfter() == null) {
            // This is the case where we have failed due to networking or other IOException error.
            isFailoverException = false;
        } else {
            // This is the case where the server has returned a 503.
            // This is done when we want to do fast failover because we aren't the leader or we are shutting down.
            isFailoverException = true;
        }
        synchronized (this) {
            // Only fail over if this failure was to the current server.
            // This means that no one on another thread has failed us over already.
            if (mostRecentServerIndex.get() != null && mostRecentServerIndex.get() == failoverCount.get()) {
                long failures = failuresSinceLastSwitch.incrementAndGet();
                if (isFailoverException || failures >= failuresBeforeSwitching) {
                    if (isFailoverException) {
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

        checkAndHandleFailure(e);
        return;
    }

    private void checkAndHandleFailure(RetryableException e) {
        final long failoverStartTime = startTimeOfFastFailover.get();
        final long currentTime = System.currentTimeMillis();
        boolean failedDueToFailover = failoverStartTime != 0 && (currentTime - failoverStartTime) > fastFailoverTimeoutMillis;
        boolean failedDueToNumSwitches = numSwitches.get() >= numServersToTryBeforeFailing;

        if (failedDueToFailover) {
            log.warn("This connection has been instructed to fast failover for " +
                    TimeUnit.MILLISECONDS.toSeconds(fastFailoverTimeoutMillis) +
                    " seconds without establishing a successful connection." +
                    " The remote hosts have been in a fast failover state for too long.");
        } else if (failedDueToNumSwitches) {
            log.warn("This connection has tried " + numServersToTryBeforeFailing
                    + " hosts each " + failuresBeforeSwitching + " times and has failed out.", e);
        }

        if (failedDueToFailover || failedDueToNumSwitches) {
            throw e;
        }
    }

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
        return getServer(indexToHit);
    }

    private String getServer(int failoverVersion) {
        return servers.get(failoverVersion % servers.size());
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
                    sucessfulCall();
                }
                return response;
            }
        };
    }
}
