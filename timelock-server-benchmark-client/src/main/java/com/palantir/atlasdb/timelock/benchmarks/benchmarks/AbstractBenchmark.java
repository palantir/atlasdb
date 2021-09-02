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
package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

public abstract class AbstractBenchmark {

    private static final SafeLogger log = SafeLoggerFactory.get(AbstractBenchmark.class);

    private final int numClients;
    private final int requestsPerClient;

    private final ExecutorService executor;
    private final AtomicLongArray times;
    private volatile long totalTime;
    private volatile Throwable error = null;
    private volatile long[] sortedTimes;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final CountDownLatch completionLatch;

    protected AbstractBenchmark(int numClients, int requestsPerClient) {
        this.numClients = numClients;
        this.requestsPerClient = requestsPerClient;

        executor = Executors.newFixedThreadPool(numClients);

        int totalNumberOfRequests = numClients * requestsPerClient;
        times = new AtomicLongArray(totalNumberOfRequests);
        completionLatch = new CountDownLatch(totalNumberOfRequests);
    }

    public Map<String, Object> execute() {
        setup();

        try {
            runTests();
            return getStatistics();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            cleanup();
        }
    }

    private void runTests() throws InterruptedException {
        try {
            long start = System.nanoTime();
            scheduleTests();
            waitForTestsToComplete();
            totalTime = System.nanoTime() - start;
        } finally {
            stopExecutors();
        }
    }

    private void scheduleTests() {
        for (int i = 0; i < numClients; i++) {
            executor.execute(this::runTestForSingleClient);
        }
    }

    // TODO(gmaretic): get useful errors when they occur, maybe also get logs
    private void runTestForSingleClient() {
        try {
            recordTimesForSingleClient();
        } catch (Throwable t) {
            error = t;
        }
    }

    private void recordTimesForSingleClient() {
        for (int i = 0; i < requestsPerClient && error == null; i++) {
            long duration = timeOneCall();
            times.set(counter.getAndIncrement(), duration);
            completionLatch.countDown();
        }
    }

    private void waitForTestsToComplete() throws InterruptedException {
        while (!completionLatch.await(1, TimeUnit.SECONDS)) {
            throwIfAnyCallsFailed();
            log.info("completed {} calls", SafeArg.of("counter", counter.get()));
        }
    }

    private void throwIfAnyCallsFailed() {
        if (error != null) {
            throw Throwables.propagate(error);
        }
    }

    private void stopExecutors() {
        executor.shutdownNow();
    }

    private long timeOneCall() {
        long start = System.nanoTime();
        performOneCall();
        long end = System.nanoTime();

        return end - start;
    }

    protected abstract void performOneCall();

    protected void setup() {}

    // TODO(gmaretic): implement cleanup -- truncate tables, etc.
    protected void cleanup() {}

    protected Map<String, Object> getExtraParameters() {
        return ImmutableMap.of();
    }

    private Map<String, Object> getStatistics() {
        sortTimes();

        Map<String, Object> result = new HashMap<>();
        result.put("numClients", numClients);
        result.put("requestsPerClient", requestsPerClient);
        result.put("average", getAverageTimeInMillis());
        result.put("p50", getPercentile(0.5));
        result.put("p95", getPercentile(0.95));
        result.put("p99", getPercentile(0.99));
        result.put("totalTime", totalTime / 1_000_000.0);
        result.put("throughput", getThroughput());
        result.put("name", getClass().getSimpleName());
        result.putAll(getExtraParameters());

        return result;
    }

    private void sortTimes() {
        long[] result = new long[times.length()];
        for (int i = 0; i < times.length(); i++) {
            result[i] = times.get(i);
        }
        Arrays.sort(result);

        sortedTimes = result;
    }

    private double getAverageTimeInMillis() {
        return Arrays.stream(sortedTimes).average().getAsDouble() / 1_000_000.0;
    }

    private double getPercentile(double percentile) {
        double count = sortedTimes.length;
        int index = (int) (count * percentile);
        return sortedTimes[index] / 1_000_000.0;
    }

    public double getThroughput() {
        return (double) (numClients * requestsPerClient) / (totalTime / 1_000_000_000.0);
    }
}
