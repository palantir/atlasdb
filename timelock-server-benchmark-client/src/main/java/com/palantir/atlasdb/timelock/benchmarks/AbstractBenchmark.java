/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.benchmarks;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(AbstractBenchmark.class);

    private final int numClients;
    private final int requestsPerClient;

    private final ExecutorService executor;
    private final List<Long> times = Lists.newArrayList();
    private volatile long totalTime;
    private volatile Throwable error = null;

    protected AbstractBenchmark(int numClients, int requestsPerClient) {
        this.numClients = numClients;
        this.requestsPerClient = requestsPerClient;

        executor = Executors.newFixedThreadPool(numClients);
    }

    public Map<String, Object> execute() {
        try {
            runTests();
            return getStatistics();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
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
            executor.submit(this::runTestForSingleClient);
        }
    }

    private void runTestForSingleClient() {
        try {
            recordTimesForSingleClient();
        } catch (Throwable t) {
            error = t;
        }
    }

    private void recordTimesForSingleClient() {
        List<Long> timesForClient = getTimesForSingleClient();
        synchronized (times) {
            times.addAll(timesForClient);
            if (areTestsCompleted()) {
                times.notifyAll();
            }
        }
    }

    private List<Long> getTimesForSingleClient() {
        List<Long> timesForClient = Lists.newArrayList();
        for (int i = 0; i < requestsPerClient; i++) {
            timesForClient.add(timeOneCall());
        }
        return timesForClient;
    }

    private void waitForTestsToComplete() throws InterruptedException {
        synchronized (times) {
            while (!areTestsCompleted()) {
                throwIfAnyCallsFailed();
                times.wait(500);
            }
        }
    }

    private boolean areTestsCompleted() {
        synchronized (times) {
            log.info("{} calls completed", times.size());
            return times.size() >= numClients * requestsPerClient;
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

    private Map<String, Object> getStatistics() {
        Map<String, Object> result = Maps.newHashMap();
        result.put("numClients", numClients);
        result.put("requestsPerClient", requestsPerClient);
        result.put("average", getAverageTimeInMillis());
        result.put("p50", getPercentile(0.5));
        result.put("p95", getPercentile(0.95));
        result.put("p99", getPercentile(0.99));
        result.put("totalTime", totalTime / 1_000_000.0);
        result.put("throughput", getThroughput());
        result.put("name", getClass().getSimpleName());
        return result;
    }

    private double getAverageTimeInMillis() {
        return times.stream().mapToLong(t -> t).average().getAsDouble() / 1_000_000.0;
    }

    private double getPercentile(double percentile) {
        double count = times.size();
        int n = (int)(count * percentile);
        return times.stream().sorted().skip(n).iterator().next() / 1_000_000.0;
    }

    public double getThroughput() {
        return (double)(numClients * requestsPerClient) / (totalTime / 1_000_000_000.0);
    }
}
