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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.palantir.atlasdb.http.AtlasDbErrorDecoder;
import com.palantir.atlasdb.http.TextDelegateDecoder;
import com.palantir.atlasdb.timelock.benchmarks.BenchmarksService;

import feign.Client;
import feign.Contract;
import feign.Feign;
import feign.Request;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import feign.okhttp.OkHttpClient;

public class BenchmarkRunnerBase {

    private static final String BENCHMARK_SERVER = "http://il-pg-alpha-1767557.euw1.palantir.global:9425";
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    protected static final BenchmarksService client = createClient();

    protected void runAndPrintResults(BiFunction<Integer, Integer, Map<String, Object>> test, int numClients,
            int numRequestsPerClient) {
        Map<String, Object> results = test.apply(numClients, numRequestsPerClient);
        printResults(results);
    }

    protected void printResults(Map<String, Object> results) {
        try {
            System.out.println(MAPPER.writeValueAsString(results));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected static final BenchmarksService createClient() {
        ObjectMapper mapper = new ObjectMapper();
        Contract contract = new JAXRSContract();
        Encoder encoder = new JacksonEncoder(mapper);
        Decoder decoder = new TextDelegateDecoder(new JacksonDecoder(mapper));
        ErrorDecoder errorDecoder = new AtlasDbErrorDecoder();

        okhttp3.OkHttpClient.Builder builder = new okhttp3.OkHttpClient.Builder()
                .hostnameVerifier((a, b) -> true)
                .retryOnConnectionFailure(false);
        Client client = new OkHttpClient(builder.build());

        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .client(client)
                .options(new Request.Options(10_000, 600_000))
                .target(BenchmarksService.class, BENCHMARK_SERVER);
    }

}
