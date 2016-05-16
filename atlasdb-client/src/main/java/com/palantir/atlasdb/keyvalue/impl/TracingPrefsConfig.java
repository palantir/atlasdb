/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

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

    @Override
    public void run() {
        try {
            final File TRACING_PREF_FILE = new File(System.getProperty("user.dir") + java.io.File.separatorChar + TRACING_PREF_FILENAME);

            if (TRACING_PREF_FILE.exists()) {
                try (FileInputStream fileStream = new FileInputStream(TRACING_PREF_FILE)) {
                    tracingPrefConfig.load(fileStream);
                    tracingEnabled = Boolean.parseBoolean(tracingPrefConfig.getProperty("tracing_enabled", "false"));
                    tracingProbability = Double.parseDouble(tracingPrefConfig.getProperty("trace_probability", "1.0"));
                    tracingMinDurationToTraceMillis = Integer.parseInt(tracingPrefConfig.getProperty("min_duration_to_log_ms", "0"));
                    String tableString = tracingPrefConfig.getProperty("tables_to_trace", "");
                    tracedTables = ImmutableSet.copyOf(Splitter.on(",").trimResults().split(tableString));
                } catch (IOException e) {
                    log.error("Could not load a malformed " + TRACING_PREF_FILENAME + ".");
                    loadedConfig = false;
                }
                loadedConfig = true;
            }
        } catch(Throwable t) {
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
        return false;
    }
}
