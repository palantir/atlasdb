/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration.jmx;

import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Map;

public class CassandraMetricsRetriever {
    private final CassandraJmxConnector connector;

    public CassandraMetricsRetriever(CassandraJmxConnector connector) {
        this.connector = connector;
    }

    public Object getCassandraMetric(
            String metricDomain, String metricType, String metricAttribute, Map<String, String> params) {
        Map<String, String> allParams = ImmutableMap.<String, String>builder()
                .putAll(params)
                .put("type", metricType)
                .buildOrThrow();
        try {
            return connector.getMetricFromJmx(metricDomain, metricAttribute, allParams);
        } catch (Exception e) {
            throw new SafeRuntimeException("Failed to get metric", e, SafeArg.of("metricAttribute", metricAttribute));
        }
    }
}
