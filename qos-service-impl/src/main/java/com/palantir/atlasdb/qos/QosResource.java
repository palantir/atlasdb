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

package com.palantir.atlasdb.qos;

import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.ratelimit.CassandraMetricsClientLimitMultiplier;
import com.palantir.atlasdb.qos.ratelimit.ClientLimitMultiplier;
import com.palantir.atlasdb.qos.ratelimit.OneReturningClientLimitMultiplier;

public class QosResource implements QosService {

    private Supplier<QosServiceRuntimeConfig> config;
    private final ClientLimitMultiplier clientLimitMultiplier;

    public QosResource(Supplier<QosServiceRuntimeConfig> config) {
        this.config = config;
        this.clientLimitMultiplier = getNonLiveReloadableClientLimitMultiplier();
    }

    private ClientLimitMultiplier getNonLiveReloadableClientLimitMultiplier() {
        if (config.get().qosCassandraMetricsConfig().isPresent()) {
            return CassandraMetricsClientLimitMultiplier.create(() -> {
                Preconditions.checkState(config.get().qosCassandraMetricsConfig().isPresent(),
                        "The Qos Cassandra metrics config was present before but can not be found now,"
                                + "removing this config block is not supported live.");
                return config.get().qosCassandraMetricsConfig().get();
            });
        } else {
            return OneReturningClientLimitMultiplier.create();
        }
    }

    @Override
    public long readLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = config.get().clientLimits().getOrDefault(client,
                ImmutableQosClientLimitsConfig.builder().build());
        return (long) clientLimitMultiplier.getClientLimitMultiplier(qosClientLimitsConfig.clientPriority())
                * qosClientLimitsConfig.limits().readBytesPerSecond();
    }

    @Override
    public long writeLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = config.get().clientLimits().getOrDefault(client,
                ImmutableQosClientLimitsConfig.builder().build());
        return (long) clientLimitMultiplier.getClientLimitMultiplier(qosClientLimitsConfig.clientPriority())
                * qosClientLimitsConfig.limits().writeBytesPerSecond();
    }
}
