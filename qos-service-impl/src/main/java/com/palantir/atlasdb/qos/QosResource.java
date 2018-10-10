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
package com.palantir.atlasdb.qos;

import com.palantir.atlasdb.qos.agent.QosClientConfigLoader;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.ratelimit.ClientLimitMultiplier;
import com.palantir.atlasdb.util.MetricsManager;

public class QosResource implements QosService {
    private final QosClientConfigLoader qosClientConfigLoader;
    private final ClientLimitMultiplier clientLimitMultiplier;
    private static volatile double readLimitMultiplier = 1.0;
    private static volatile double writeLimitMultiplier = 1.0;

    public QosResource(
            MetricsManager metricsManager,
            QosClientConfigLoader qosClientConfigLoader,
            ClientLimitMultiplier clientLimitMultiplier) {
        this.qosClientConfigLoader = qosClientConfigLoader;
        this.clientLimitMultiplier = clientLimitMultiplier;
        metricsManager.registerMetric(QosResource.class, "readLimitMultiplier", this::getReadLimitMultiplier);
        metricsManager.registerMetric(QosResource.class, "writeLimitMultiplier", this::getWriteLimitMultiplier);
    }

    @Override
    public long readLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = qosClientConfigLoader.getConfigForClient(client);
        readLimitMultiplier = this.clientLimitMultiplier.getClientLimitMultiplier(
        );
        return (long) (readLimitMultiplier * qosClientLimitsConfig.limits().readBytesPerSecond());
    }

    @Override
    public long writeLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = qosClientConfigLoader.getConfigForClient(client);
        writeLimitMultiplier = this.clientLimitMultiplier.getClientLimitMultiplier(
        );
        return (long) (writeLimitMultiplier * qosClientLimitsConfig.limits().writeBytesPerSecond());
    }

    private double getReadLimitMultiplier() {
        return readLimitMultiplier;
    }

    private double getWriteLimitMultiplier() {
        return writeLimitMultiplier;
    }
}
