/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.codahale.metrics.jvm;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import java.util.Collections;
import java.util.Map;

/**
 * We need to use Dropwizard metrics 3.x internally - this is the class that's needed to get
 * Dropwizard 2.0 working with metrics 3.x.
 */
public final class JvmAttributeGaugeSet implements MetricSet {
    @Override
    public Map<String, Metric> getMetrics() {
        return Collections.emptyMap();
    }
}
