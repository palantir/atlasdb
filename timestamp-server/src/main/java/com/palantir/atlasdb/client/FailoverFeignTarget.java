package com.palantir.atlasdb.client;

import java.util.List;

import feign.Request;
import feign.RequestTemplate;
import feign.RetryableException;
import feign.Retryer;
import feign.Target;

public class FailoverFeignTarget<T> implements Target<T>, Retryer {
    List<String> servers;
    Class<T> type;
    private final int maxAttempts = 20;
    private final long period = 10;
    private final long maxPeriod = 100;

    ThreadLocal<RetryState> state = new ThreadLocal<RetryState>() {
        protected FailoverFeignTarget.RetryState initialValue() {
            return new RetryState();
        }
    };

    @Override
    public void continueOrPropagate(RetryableException e) {
        if (state.get().attempt++ >= maxAttempts) {
            throw e;
        }

        long interval;
        if (e.retryAfter() != null) {
            interval = e.retryAfter().getTime() - System.currentTimeMillis();
            if (interval > maxPeriod) {
                interval = maxPeriod;
            }
            if (interval < 0) {
                return;
            }
        } else {
            interval = nextMaxInterval();
        }
        try {
            Thread.sleep(interval);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    long nextMaxInterval() {
      long interval = (long) (period * Math.pow(1.5, state.get().attempt - 1));
      return interval > maxPeriod ? maxPeriod : interval;
    }

    @Override
    public Retryer clone() {
        state.remove();
        return this;
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String url() {
        return null;
    }

    @Override
    public Request apply(RequestTemplate input) {
        // TODO: get from target state
        if (input.url().indexOf("http") != 0) {
            input.insert(0, url());
        }
        return input.request();
    }

    static class RetryState {
        int attempt = 1;
    }

}
