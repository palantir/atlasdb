/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@JsonDeserialize(as = ImmutableCassandraTracingConfig.class)
@JsonSerialize(as = ImmutableCassandraTracingConfig.class)
public abstract class CassandraTracingConfig {
    private static final Logger log = LoggerFactory.getLogger(CassandraTracingConfig.class);
    private static final AtomicBoolean loggedWarning = new AtomicBoolean();

    @JsonProperty("enabled")
    @Value.Default
    public boolean enabled() {
        return false;
    }

    @JsonProperty("trace-probability")
    @Value.Default
    public double traceProbability() {
        return 1.0;
    }

    @JsonProperty("tables-to-trace")
    public abstract Set<String> tablesToTrace();

    @JsonProperty("min-duration-to-log-ms")
    @Value.Default
    public int minDurationToLogMs() {
        return 0;
    }

    public boolean shouldTraceQuery(String tablename, Random random) {
        if (!enabled()) {
            return false;
        }
        if (tablesToTrace().contains(tablename)) {
            if (traceProbability() == 1.0) {
                return true;
            } else {
                if (random.nextDouble() <= traceProbability()) {
                    return true;
                }
            }
        }
        if (tablesToTrace().isEmpty()) {
            return true; // accept enabled = true but no tables specified to mean trace all tables
        }
        return false;
    }

    @Value.Check
    protected void check() {
        // This does not actually validate anything but mimics the old behavior where enabling tracing
        // would log a warning.
        if (enabled()) {
            if (loggedWarning.compareAndSet(false, true)) {
                log.warn(
                        "Tracing is enabled. This incurs a large performance hit and should only be used for short"
                                + " periods of debugging. [trace-probability = {}, min-duration-to-log-ms = {},"
                                + " tables-to-trace = {}]",
                        SafeArg.of("trace-probability", traceProbability()),
                        SafeArg.of("min-duration-to-log-ms", minDurationToLogMs()),
                        UnsafeArg.of("tables-to-trace", tablesToTrace()));
            }
        }
    }
}
