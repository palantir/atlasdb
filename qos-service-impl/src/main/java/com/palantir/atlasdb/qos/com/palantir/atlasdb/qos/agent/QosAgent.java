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
package com.palantir.atlasdb.qos.com.palantir.atlasdb.qos.agent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.palantir.atlasdb.qos.QosResource;
import com.palantir.atlasdb.qos.config.QosServiceInstallConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.ratelimit.ClientLimitMultiplier;
import com.palantir.atlasdb.qos.ratelimit.OneReturningClientLimitMultiplier;

public class QosAgent {
    private final Supplier<QosServiceRuntimeConfig> runtimeConfigSupplier;
    private final QosServiceInstallConfig installConfig;
    private ScheduledExecutorService managedMetricsLoaderExecutor;
    private final Consumer<Object> registrar;

    public QosAgent(Supplier<QosServiceRuntimeConfig> runtimeConfigSupplier,
            QosServiceInstallConfig installConfig,
            ScheduledExecutorService managedMetricsLoaderExecutor,
            Consumer<Object> registrar) {
        this.runtimeConfigSupplier = runtimeConfigSupplier;
        this.installConfig = installConfig;
        this.managedMetricsLoaderExecutor = managedMetricsLoaderExecutor;
        this.registrar = registrar;
    }

    public void createAndRegisterResources() {
        QosClientConfigLoader qosClientConfigLoader = QosClientConfigLoader.create(
                () -> runtimeConfigSupplier.get().clientLimits());
        ClientLimitMultiplier clientLimitMultiplier = createClientLimitMultiplier();
        registrar.accept(new QosResource(qosClientConfigLoader, clientLimitMultiplier));
    }

    private ClientLimitMultiplier createClientLimitMultiplier() {
        // "Temporary", until we have a stub implementation for MetricsService
        return OneReturningClientLimitMultiplier.create();
    }
}
