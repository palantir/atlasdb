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
package com.palantir.atlasdb.performance.benchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import com.palantir.common.remoting.HeaderAccessUtils;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MediaType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
public class HttpBenchmarks {
    private static final String LOWERCASE_CONTENT_TYPE = HttpHeaders.CONTENT_TYPE.toLowerCase();

    // The headers here are all in lowercase, following OkHttp3.3.0+
    private static final Map<String, Collection<String>> HEADERS =
            ImmutableMap.<String, Collection<String>>builder()
                    .put(HttpHeaders.ACCEPT.toLowerCase(), ImmutableList.of(MediaType.APPLICATION_JSON))
                    .put(HttpHeaders.ACCEPT_ENCODING.toLowerCase(), ImmutableList.of("UTF-8"))
                    .put(HttpHeaders.CACHE_CONTROL.toLowerCase(), ImmutableList.of("no cache"))
                    .put(HttpHeaders.CONTENT_TYPE.toLowerCase(), ImmutableList.of(MediaType.TEXT_PLAIN))
                    .put(HttpHeaders.FROM.toLowerCase(), ImmutableList.of("TimeLock"))
                    .put(HttpHeaders.USER_AGENT.toLowerCase(), ImmutableList.of("atlasdb/atlasdb-atlasdb"))
                    .put(HttpHeaders.SET_COOKIE.toLowerCase(), ImmutableList.of("cookie"))
                    .put(HttpHeaders.EXPECT.toLowerCase(), ImmutableList.of("12391572384129734"))
                    .build();

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public void parseHttpHeaders(Blackhole blackhole) {
        blackhole.consume(HeaderAccessUtils.shortcircuitingCaseInsensitiveContainsEntry(
                HEADERS,
                LOWERCASE_CONTENT_TYPE,
                MediaType.TEXT_PLAIN));
    }
}
