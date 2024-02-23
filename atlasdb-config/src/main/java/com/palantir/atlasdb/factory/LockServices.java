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

package com.palantir.atlasdb.factory;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockService;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.client.RemoteLockServiceAdapter;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.concurrent.ExecutorService;

public final class LockServices {
    // No need for services config block since timelock server uris come from runtime config.
    private static final Refreshable<ServicesConfigBlock> DUMMY_SERVICES_CONFIG =
            Refreshable.only(ServicesConfigBlock.builder().build());

    private LockServices() {
        // static factory
    }

    public static LockService createLockServiceClient(
            Refreshable<ServerListConfig> timeLockServerListConfig,
            ExecutorService lockClientExecutor,
            UserAgent userAgent,
            MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry,
            String timelockNamespace) {
        DialogueClients.ReloadingFactory reloadingFactory =
                DialogueClients.create(DUMMY_SERVICES_CONFIG).withBlockingExecutor(lockClientExecutor);
        AtlasDbDialogueServiceProvider serviceProvider = AtlasDbDialogueServiceProvider.create(
                timeLockServerListConfig, reloadingFactory, userAgent, taggedMetricRegistry);
        return wrapWithDefaultDecorators(
                createRawLockServiceClient(serviceProvider, metricRegistry, timelockNamespace));
    }

    public static LockService createRawLockServiceClient(
            AtlasDbDialogueServiceProvider provider, MetricRegistry metricRegistry, String timelockNamespace) {
        LockRpcClient lockRpcClient = provider.getLockRpcClient();
        return AtlasDbMetrics.instrumentTimed(
                metricRegistry, LockService.class, RemoteLockServiceAdapter.create(lockRpcClient, timelockNamespace));
    }

    public static LockService wrapWithDefaultDecorators(LockService lockService) {
        return LockRefreshingLockService.create(lockService);
    }
}
