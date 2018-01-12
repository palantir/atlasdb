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

import com.palantir.atlasdb.qos.com.palantir.atlasdb.qos.agent.QosClientConfigLoader;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.ratelimit.ClientLimitMultiplier;
import com.palantir.atlasdb.util.MetricsManager;

public class QosResource implements QosService {
    private final QosClientConfigLoader qosClientConfigLoader;
    private final ClientLimitMultiplier clientLimitMultiplier;
    private static volatile double readLimitMultiplier = 1.0;
    private static volatile double writeLimitMultiplier = 1.0;

    public QosResource(QosClientConfigLoader qosClientConfigLoader, ClientLimitMultiplier clientLimitMultiplier) {
        this.qosClientConfigLoader = qosClientConfigLoader;
        this.clientLimitMultiplier = clientLimitMultiplier;
        new MetricsManager().registerMetric(QosResource.class, "readLimitMultiplier", this::getReadLimitMultiplier);
        new MetricsManager().registerMetric(QosResource.class, "writeLimitMultiplier", this::getWriteLimitMultiplier);
    }

    @Override
    public long readLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = qosClientConfigLoader.getConfigForClient(client);
        readLimitMultiplier = this.clientLimitMultiplier.getClientLimitMultiplier(
                qosClientLimitsConfig.clientPriority());
        return (long) (readLimitMultiplier * qosClientLimitsConfig.limits().readBytesPerSecond());
    }

    @Override
    public long writeLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = qosClientConfigLoader.getConfigForClient(client);
        writeLimitMultiplier = this.clientLimitMultiplier.getClientLimitMultiplier(
                qosClientLimitsConfig.clientPriority());
        return (long) (writeLimitMultiplier * qosClientLimitsConfig.limits().writeBytesPerSecond());
    }

    private double getReadLimitMultiplier() {
        return readLimitMultiplier;
    }

    private double getWriteLimitMultiplier() {
        return writeLimitMultiplier;
    }
}
