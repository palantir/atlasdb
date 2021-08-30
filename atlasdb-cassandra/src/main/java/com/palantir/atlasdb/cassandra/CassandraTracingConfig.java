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
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableCassandraTracingConfig.class)
@JsonSerialize(as = ImmutableCassandraTracingConfig.class)
public abstract class CassandraTracingConfig {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraTracingConfig.class);
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

    @JsonProperty("min-duration-to-log")
    @Value.Default
    public HumanReadableDuration minDurationToLog() {
        return HumanReadableDuration.milliseconds(0);
    }

    @Value.Check
    protected void check() {
        if (enabled()) {
            Preconditions.checkArgument(
                    traceProbability() > 0, "trace-probability must be greater than 0 if tracing is enabled");

            // Just log a warning once. In witchcraft runtime config is reparsed every second which means this would
            // log every second.
            if (loggedWarning.compareAndSet(false, true)) {
                log.warn(
                        "Tracing is enabled. This incurs a large performance hit and should only be used for short"
                                + " periods of debugging. [trace-probability = {}, min-duration-to-log-ms = {},"
                                + " tables-to-trace = {}]",
                        SafeArg.of("trace-probability", traceProbability()),
                        SafeArg.of("min-duration-to-log", minDurationToLog()),
                        UnsafeArg.of("tables-to-trace", tablesToTrace()));
            }
        }
    }
}
