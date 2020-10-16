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
package com.palantir.atlasdb.util;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.stream.Collectors;

public final class MetricNameUtils {

    private static final int MAX_ALLOWED_TAGS = 10;

    private MetricNameUtils() {
        // utility class
    }

    private static final String DELIMITER = ";";
    private static final String EQUALS = "=";

    public static String getMetricName(String metricName, Map<String, String> tags) {
        Preconditions.checkArgument(
                tags.size() <= MAX_ALLOWED_TAGS,
                "Too many tags set on the metric %s. " + "Maximum allowed number of tags is %s, found %s.",
                metricName,
                MAX_ALLOWED_TAGS,
                tags.size());

        validateMetricComponentName(metricName, "metric");
        validateMetricTagNames(tags);

        return tags.entrySet().stream()
                .map(e -> e.getKey() + EQUALS + e.getValue())
                .collect(Collectors.joining(DELIMITER, metricName + DELIMITER, ""));
    }

    private static void validateMetricTagNames(Map<String, String> tags) {
        tags.forEach((key, value) -> {
            validateMetricComponentName(key, "tag key");
            validateMetricComponentName(value, "tag value");
        });
    }

    private static void validateMetricComponentName(String metricName, String type) {
        if (metricName.contains(DELIMITER)) {
            throw new IllegalArgumentException(
                    String.format("The %s name: %s contains the forbidden character: %s", type, metricName, DELIMITER));
        } else if (metricName.contains(EQUALS)) {
            throw new IllegalArgumentException(
                    String.format("The %s name: %s contains the forbidden character: %s", type, metricName, EQUALS));
        }
    }
}
