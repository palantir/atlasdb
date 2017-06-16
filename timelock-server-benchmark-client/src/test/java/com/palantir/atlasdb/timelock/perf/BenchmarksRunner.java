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

package com.palantir.atlasdb.timelock.perf;

import java.util.Map;
import java.util.function.BiFunction;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.benchmarks.BenchmarksService;

/**
 * Note that there is no warmup time included in any of these tests, so if the server has just been started you'll want
 * to execute many requests until the results stabilize (give the JIT compiler time to optimize).
 */
public class BenchmarksRunner {

    private static final String PERF_TEST_SERVER = "http://il-pg-alpha-1086751.usw1.palantir.global:9425";
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final BenchmarksService client = AtlasDbHttpClients.createProxy(
            Optional.absent(), PERF_TEST_SERVER, BenchmarksService.class, "perf-test-runner");

    @Test
    public void timestamp() {
        runAndPrintResults(client::timestamp, 8, 1000);
    }

    @Test
    public void writeTransaction() {
        runAndPrintResults(client::writeTransaction, 10, 100);
    }

    @Test
    public void readTransaction() {
        runAndPrintResults(client::readTransaction, 1, 500);
    }

    @Test
    public void kvsCas() {
        runAndPrintResults(client::kvsCas, 1, 5000);
    }

    @Test
    public void kvsWrite() {
        runAndPrintResults(client::kvsWrite, 1, 1000);
    }

    @Test
    public void kvsRead() {
        runAndPrintResults(client::kvsRead, 1, 5000);
    }

    @Test
    public void contendedWriteTransaction() {
        runAndPrintResults(client::contendedWriteTransaction, 2000, 1);
    }

    private void runAndPrintResults(BiFunction<Integer, Integer, Map<String, Object>> test, int numClients, int numRequestsPerClient) {
        Map<String, Object> results = test.apply(numClients, numRequestsPerClient);
        printResults(results);
    }

    private void printResults(Map<String, Object> results) {
        try {
            System.out.println(MAPPER.writeValueAsString(results));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
