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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingPrefsConfig implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TracingPrefsConfig.class);
    private static final String TRACING_PREF_FILENAME = "atlas_tracing.prefs";

    private final Random random = new Random();
    private volatile boolean loadedConfig = false;
    private volatile boolean tracingEnabled = false;
    private volatile double tracingProbability = 1.0;
    private volatile int tracingMinDurationToTraceMillis = 0;
    private volatile Set<String> tracedTables;
    private final Properties tracingPrefConfig = new Properties();

    public static TracingPrefsConfig create() {
        TracingPrefsConfig tracingConfig = new TracingPrefsConfig();
        tracingConfig.run();
        return tracingConfig;
    }

    @Override
    public void run() {
        try {
            final File tracingPrefsFile = new File(
                    System.getProperty("user.dir") + File.separatorChar + TRACING_PREF_FILENAME);

            if (tracingPrefsFile.exists()) {
                try (FileInputStream fileStream = new FileInputStream(tracingPrefsFile)) {
                    tracingPrefConfig.load(fileStream);
                    tracingEnabled = Boolean.parseBoolean(tracingPrefConfig.getProperty("tracing_enabled", "false"));
                    tracingProbability = Double.parseDouble(tracingPrefConfig.getProperty("trace_probability", "1.0"));
                    tracingMinDurationToTraceMillis = Integer.parseInt(
                            tracingPrefConfig.getProperty("min_duration_to_log_ms", "0"));
                    String tableString = tracingPrefConfig.getProperty("tables_to_trace", "");
                    tracedTables = ImmutableSet.copyOf(Splitter.on(",").trimResults().split(tableString));
                    if (tracingEnabled && !loadedConfig) { // only log leading edge event
                        log.warn("Successfully loaded an {} file."
                                + " This incurs a large performance hit and"
                                + " should only be used for short periods of debugging."
                                + " [tracing_enabled = {}, trace_probability = {}, min_duration_to_log_ms = {}, "
                                + "tables_to_trace = {}]",
                                TRACING_PREF_FILENAME,
                                tracingEnabled,
                                tracingProbability,
                                tracingMinDurationToTraceMillis,
                                tracedTables);
                    }
                    loadedConfig = true;
                } catch (IOException e) {
                    log.error("Could not load a malformed " + TRACING_PREF_FILENAME + ".");
                    loadedConfig = false;
                }
            } else {
                loadedConfig = false;
            }
        } catch (Throwable t) {
            log.error("Error occurred while refreshing {}: {}", TRACING_PREF_FILENAME, t, t);
        }
    }

    public int getMinimumDurationToTraceMillis() {
        return tracingMinDurationToTraceMillis;
    }

    public boolean shouldTraceQuery(final String tablename) {
        if (!loadedConfig) {
            return false;
        }
        if (!tracingEnabled) {
            return false;
        }
        if (tracedTables.contains(tablename)) {
            if (tracingProbability == 1.0) {
                return true;
            } else {
                if (random.nextDouble() <= tracingProbability) {
                    return true;
                }
            }
        }
        if (tracedTables.isEmpty()) {
            return true; // accept tracing_enabled = true but no tables specified to mean trace all tables
        }
        return false;
    }
}
