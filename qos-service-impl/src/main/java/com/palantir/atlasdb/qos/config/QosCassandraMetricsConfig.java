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

package com.palantir.atlasdb.qos.config;

import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.remoting.api.config.service.ServiceConfiguration;

@Value.Immutable
@JsonSerialize(as = ImmutableQosCassandraMetricsConfig.class)
@JsonDeserialize(as = ImmutableQosCassandraMetricsConfig.class)
public abstract class QosCassandraMetricsConfig {
    public abstract ServiceConfiguration cassandraServiceConfig();

    public abstract List<CassandraHealthMetric> cassandraHealthMetrics();

    public abstract ThrottlingStrategy.ThrottlingStrategies throttlingStrategy();
}
