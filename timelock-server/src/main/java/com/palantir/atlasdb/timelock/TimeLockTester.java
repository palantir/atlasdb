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

package com.palantir.atlasdb.timelock;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.timestamp.TimestampService;

public class TimeLockTester {
    private static final ImmutableSet<String> SERVERS = ImmutableSet.of(
            "http://localhost:8421",
            "http://localhost:8423",
            "http://localhost:8425",
            "http://localhost:8427",
            "http://localhost:8429");
    private static final ImmutableSet<String> CLIENTS =
            ImmutableSet.of("foundry-catalog", "small1", "small2", "small3");

    private static final Map<String, TimestampService> TIMESTAMP_SERVICES = CLIENTS.stream()
            .collect(Collectors.toMap(
                    Function.identity(),
                    x -> AtlasDbHttpClients.createProxyWithFailover(
                            Optional.empty(),
                            SERVERS.stream()
                                    .map(y -> y + "/" + x)
                                    .collect(Collectors.toList()),
                            TimestampService.class)));

    private static final ImmutableMap<String, Integer> THREADS_FOR_CLIENTS = ImmutableMap.of(
            "cat", 10,
            "cat2", 2,
            "cat3", 2,
            "cat4", 2);

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(16);

    public static void main(String[] args) throws InterruptedException {
        for (Map.Entry<String, Integer> entry : THREADS_FOR_CLIENTS.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                EXECUTOR.scheduleAtFixedRate(
                        () -> TIMESTAMP_SERVICES.get(entry.getKey()).getFreshTimestamp(),
                        0L,
                        500L,
                        TimeUnit.MILLISECONDS);
            }
        }

        Thread.sleep(Long.MAX_VALUE);
    }
}
