/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock;

import java.io.FileReader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk7.Jdk7Module;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.remoting.ssl.SslSocketFactories;
import com.palantir.timestamp.TimestampService;

final class TimelockTester {
    private static final Logger log = LoggerFactory.getLogger(TimelockTester.class);

    private TimelockTester() {
        //no public constructor
    }


    public static void main(String[] args) throws Exception {

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new Jdk7Module());

        // read config

        FileReader configurationFile = new FileReader(args[0]);
        TesterConfiguration testerConfiguration = mapper.readValue(configurationFile, TesterConfiguration.class);

        Set<String> paths = testerConfiguration.clients();
        long numThreads = testerConfiguration.numThreads();
        long numClients = testerConfiguration.numClients();

        SslConfiguration sslConfiguration = testerConfiguration.sslConfiguration();
        SSLSocketFactory sslSocketFactory = SslSocketFactories.createSslSocketFactory(sslConfiguration);

        CyclicBarrier barrier = new CyclicBarrier((int) (numThreads * numClients));

        for (int currentClientNum = 0; currentClientNum < numClients; currentClientNum++) {
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool((int) numThreads);
            TimestampService proxy = AtlasDbHttpClients.createProxyWithFailover(
                    Optional.of(sslSocketFactory),
                    paths,
                    TimestampService.class);
            for (int i = 0; i < numThreads; i++) {
                long qpsForCurrentClientPerThread = testerConfiguration.queriesPerSecond().get(currentClientNum)
                        / numThreads;
                scheduler.scheduleAtFixedRate(
                        new IdRequestor(barrier, proxy),
                        testerConfiguration.initialDelay(),
                        1000_000 / qpsForCurrentClientPerThread,
                        TimeUnit.MICROSECONDS);
            }
            scheduler.awaitTermination(10, TimeUnit.MINUTES);
        }
    }

    private static class IdRequestor implements Runnable {
        private final CyclicBarrier barrier;
        private TimestampService timestampService;

        IdRequestor(CyclicBarrier barrier, TimestampService timestampService) {
            this.barrier = barrier;
            this.timestampService = timestampService;
        }

        @Override
        public void run() {
            List<Long> timestamps = Lists.newArrayList();
            Stopwatch stopwatch = Stopwatch.createUnstarted();

            // Start barrier
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                log.error("Failed barrier", e);
            }
            stopwatch.start();

            try {
                long ts = timestampService.getFreshTimestamp();
                timestamps.add(ts); // Block dead code optimisation

                if (timestamps.size() % 10000 == 0) {
                    System.out.println(
                            "[" + new DateTime() + "] " + timestamps.size() + " timestamps retrieved by a thread.");
                }
            } catch (Exception e) {
                // e.g. connect exception, if the server died
                log.error("Failed ", e);
            }

            stopwatch.stop();
            System.out.println(timestamps.size() + " in " + stopwatch.elapsed(TimeUnit.MILLISECONDS)); // Throughput
        }
    }
}
