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

package com.palantir.timelock.partition;

import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class DropwizardClientMetricsService implements ClientMetricsService {
    private static final Pattern PAXOS_ACCEPTOR_PATTERN =
            Pattern.compile("^com\\.palantir\\.atlasdb\\.timelock\\.paxos\\.ManagedTimestampService\\.(.*)\\."
                    + "getFreshTimestamp$");

    @Override
    public Map<String, Double> getLoadMetrics() {
        Map<String, Double> values = Maps.newHashMap();
        SortedMap<String, Timer> timers = AtlasDbMetrics.getMetricRegistry().getTimers();

        // TODO (jkong): Wow, this is bad.
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            Matcher matcher = PAXOS_ACCEPTOR_PATTERN.matcher(entry.getKey());
            if (matcher.find()) {
                String client = matcher.group(1);
                values.put(client, entry.getValue().getFiveMinuteRate());
            }
        }
        return values;
    }
}
