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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timestamp.TimestampService;

public class MultiServiceTimestampBenchmark {
    private static final Logger log = LoggerFactory.getLogger(AbstractBenchmark.class);

    private final Map<String, Integer> clientToNumClients;
    private final Map<String, Integer> clientToRequestsPerClient;
    private final Set<String> timelocks;
    private final Map<String, TimestampService> clientToTimestampServices;

    protected final ExecutorService executor;
    private final List<Long> times = Lists.newArrayList();
    private volatile long totalTime;
    private volatile Throwable error = null;

    protected MultiServiceTimestampBenchmark(Map<String, Integer> clientToNumClients,
            Map<String, Integer> clientToRequestsPerClient,
            AtlasDbConfig config) {
        this.clientToNumClients = clientToNumClients;
        this.clientToRequestsPerClient = clientToRequestsPerClient;
        this.timelocks = config.timelock().get().serversList().servers();

        clientToTimestampServices = Maps.newHashMap();
        for (String s : clientToNumClients.keySet()) {
            clientToTimestampServices.put(s,
                    AtlasDbHttpClients.createProxyWithFailover(
                            config.timelock().get().serversList().sslConfiguration().map(SslSocketFactories::createSslSocketFactory),
                            timelocks,
                            TimestampService.class));
        }

        executor = Executors.newFixedThreadPool(clientToNumClients.values().stream().mapToInt(x -> x).sum());
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
        for (Map.Entry<String, Integer> numClients : clientToNumClients.entrySet()) {
            for (int i = 0; i < numClients.getValue(); i++) {
                executor.submit(() -> runTestForSingleClient(numClients.getKey()));
            }
        }
    }

    private void runTestForSingleClient(String clientName) {
        try {
            recordTimesForSingleClient(clientName);
        } catch (Throwable t) {
            error = t;
        }
    }

    private void recordTimesForSingleClient(String clientName) {
        List<Long> timesForClient = getTimesForSingleClient(clientName);
        synchronized (times) {
            times.addAll(timesForClient);
            if (areTestsCompleted()) {
                times.notifyAll();
            }
        }
    }

    private List<Long> getTimesForSingleClient(String clientName) {
        List<Long> timesForClient = Lists.newArrayList();
        int requests = clientToRequestsPerClient.get(clientName);
        TimestampService service = clientToTimestampServices.get(clientName);
        for (int i = 0; i < requests; i++) {
            timesForClient.add(timeOneCall(service));
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
            int total = getTotalRequests();
            return times.size() >= total;
        }
    }

    private int getTotalRequests() {
        int total = 0;
        for (String s : clientToNumClients.keySet()) {
            total += (clientToNumClients.get(s) * clientToRequestsPerClient.get(s));
        }
        return total;
    }

    private void throwIfAnyCallsFailed() {
        if (error != null) {
            throw Throwables.propagate(error);
        }
    }

    private void stopExecutors() {
        executor.shutdownNow();
    }


    private long timeOneCall(TimestampService service) {
        long start = System.nanoTime();
        performOneCall(service);
        long end = System.nanoTime();

        return end - start;
    }

    protected void performOneCall(TimestampService timestampService) {
        long timestamp = timestampService.getFreshTimestamp();
        assert timestamp > 0;
    }

    private Map<String, Object> getStatistics() {
        Map<String, Object> result = Maps.newHashMap();
        result.put("average", getAverageTimeInMillis());
        result.put("p50", getPercentile(0.5));
        result.put("p95", getPercentile(0.95));
        result.put("p99", getPercentile(0.99));
        result.put("p999", getPercentile(0.999));
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
        return (double)(getTotalRequests()) / (totalTime / 1_000_000_000.0);
    }

}
