/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.performance.benchmarks.endpoint;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.timestamp.TimestampService;

@State(Scope.Benchmark)
public class TimestampServiceEndpoint {

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;
    private TimestampService timestampService;

    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.services = conn.connect();
        this.timestampService = services.getTimestampService();
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.connector.close();
    }
}

