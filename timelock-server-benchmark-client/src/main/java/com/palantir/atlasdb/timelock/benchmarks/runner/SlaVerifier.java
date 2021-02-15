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
package com.palantir.atlasdb.timelock.benchmarks.runner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.BeforeClass;
import org.junit.Test;

public class SlaVerifier extends BenchmarkRunnerBase {

    @BeforeClass
    public static void warmUp() {
        client.timestamp(1, 20_000);
        client.timestamp(16, 2_000);
        client.transactionWriteRows(1, 100, 1, 100);
    }

    @Test
    public void timestampSingleThread() {
        Map<String, Object> results = client.timestamp(1, 10_000);
        assertThat((Double) results.get("throughput")).isGreaterThan(500);
        assertThat((Double) results.get("average")).isLessThan(2.0);
    }

    @Test
    public void timestampMediumLoad() {
        Map<String, Object> results = client.timestamp(16, 5_000);
        assertThat((Double) results.get("throughput")).isGreaterThan(4_000);
        assertThat((Double) results.get("average")).isLessThan(3.5);
    }

    @Test
    public void timestampHighLoad() {
        Map<String, Object> results = client.timestamp(32, 3_000);
        assertThat((Double) results.get("throughput")).isGreaterThan(7_500);
        assertThat((Double) results.get("average")).isLessThan(5.0);
    }

    @Test
    public void timestampBurst() {
        Map<String, Object> results = client.timestamp(4096, 10);
        assertThat((Double) results.get("totalTime")).isLessThan(60_000);
    }

    @Test
    public void writeTransactionHighContentionBurst() {
        Map<String, Object> results = client.transactionWriteContended(4096, 1);
        assertThat((Double) results.get("totalTime")).isLessThan(60_000);
    }

    @Test
    public void writeTransactionStripedHighContentionBurst() {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Map<String, Object>>> futures = IntStream.range(0, 8)
                .mapToObj(i -> executor.submit(() -> client.transactionWriteContended(512, 1)))
                .collect(Collectors.toList());

        futures.stream().map(this::getUnchecked).forEach(results -> {
            assertThat((Double) results.get("totalTime")).isLessThan(120_000);
        });
    }

    private Map<String, Object> getUnchecked(Future<Map<String, Object>> future) {
        try {
            return future.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
