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

package com.palantir.atlasdb.sweep.metrics;

import org.immutables.value.Value;

import com.codahale.metrics.MetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@Value.Immutable
public abstract class SweepMetricConfig {
    public abstract String namePrefix();
    public abstract MetricRegistry metricRegistry();
    public abstract TaggedMetricRegistry taggedMetricRegistry();
    public abstract UpdateEvent updateEvent();
    public abstract boolean tagWithTableName();
    public abstract SweepMetricAdapter<?> metricAdapter();

    @Value.Derived
    public String name() {
        return namePrefix() + metricAdapter().getNameComponent() + updateEvent().getNameComponent();
    }
}
