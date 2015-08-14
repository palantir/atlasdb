package com.palantir.atlasdb.client;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import feign.Request;
import feign.RequestTemplate;
import feign.RetryableException;
import feign.Retryer;
import feign.Target;

public class FailoverFeignTarget<T> implements Target<T>, Retryer {
    List<String> servers;
    Class<T> type;
    AtomicInteger failoverCount = new AtomicInteger();
    private final int maxAttempts = 20;
    private final long period = 10;
    private final long maxBackoffMillis = 1000;
    private final int failuresBeforeSwitching = 5;
    private final int numServersToTryBeforeFailing = 8;

    private final AtomicLong failuresSinceLastSwitch = new AtomicLong();
    private final AtomicLong numSwitches = new AtomicLong();

    ThreadLocal<Integer> mostRecentServerIndex = new ThreadLocal<Integer>();

    @Override
    public void continueOrPropagate(RetryableException e) {
        if (e.retryAfter() == null) {
            // This is the case where we have failed due to networking or other IOException error.
        } else {
            // This is the case where the server has returned a 503.
            // This is done when we want to do fast failover because we aren't the leader.
        }
        return;
//        if (state.get().attempt++ >= maxAttempts) {
//            throw e;
//        }
//
//        long interval;
//        if (e.retryAfter() != null) {
//            interval = e.retryAfter().getTime() - System.currentTimeMillis();
//            if (interval > f) {
//                interval = maxPeriod;
//            }
//            if (interval < 0) {
//                return;
//            }
//        } else {
//            interval = nextMaxInterval();
//        }
//        try {
//            Thread.sleep(interval);
//        } catch (InterruptedException ignored) {
//            Thread.currentThread().interrupt();
//        }
    }

    long nextMaxInterval() {
//      long interval = (long) (period * Math.pow(1.5, state.get().attempt - 1));
//      return interval > maxPeriod ? maxPeriod : interval;
      return 0;
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
}
