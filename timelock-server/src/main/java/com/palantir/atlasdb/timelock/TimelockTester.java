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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk7.Jdk7Module;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
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

        Set<String> paths = testerConfiguration.paths();
        long numThreads = testerConfiguration.numThreads();
        long numClients = testerConfiguration.numClients();

        SslConfiguration sslConfiguration = testerConfiguration.sslConfiguration();
        SSLSocketFactory sslSocketFactory = SslSocketFactories.createSslSocketFactory(sslConfiguration);
        // spawn requests
        for (int currentClientNum = 0; currentClientNum < numClients; currentClientNum++) {
            TimestampService proxy = AtlasDbHttpClients.createProxyWithFailover(
                    Optional.of(sslSocketFactory),
                    paths,
                    TimestampService.class);

            for (int i = 0; i < numThreads; i++) {
                ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                long qpsForCurrentClientPerThread = testerConfiguration.queriesPerSecond().get(currentClientNum)
                        / numThreads;
                scheduledExecutorService.scheduleAtFixedRate(
                        new IdRequestor(proxy),
                        1000,
                        1000_000 / qpsForCurrentClientPerThread,
                        TimeUnit.MICROSECONDS);
            }
        }

        // block indefinitely
        while (true) {
        }
    }

    private static class IdRequestor implements Runnable {
        private TimestampService timestampService;

        IdRequestor(TimestampService timestampService) {
            this.timestampService = timestampService;
        }

        @Override
        public void run() {
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            stopwatch.start();

            try {
                timestampService.getFreshTimestamp();
            } catch (Exception e) {
                // e.g. connect exception, if the server died
                log.error("Failed ", e);
            }

            stopwatch.stop();
            System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS)); // Throughput
            System.out.flush();
        }
    }
}
