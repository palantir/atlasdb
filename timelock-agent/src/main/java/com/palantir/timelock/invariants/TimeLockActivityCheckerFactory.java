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

package com.palantir.timelock.invariants;

import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.refreshable.Refreshable;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;
import java.util.List;
import java.util.stream.Collectors;

public class TimeLockActivityCheckerFactory {
    private final ClusterConfiguration cluster;
    private final MetricsManager metricsManager;
    private final UserAgent userAgent;

    public TimeLockActivityCheckerFactory(
            ClusterConfiguration cluster, MetricsManager metricsManager, UserAgent userAgent) {
        this.cluster = cluster;
        this.metricsManager = metricsManager;
        this.userAgent = userAgent;
    }

    public List<TimeLockActivityChecker> getTimeLockActivityCheckers() {
        return cluster.clusterMembers().stream()
                .map(this::createServiceCreatorForRemote)
                .map(creator -> creator.createService(ConjureTimelockService.class))
                .map(TimeLockActivityChecker::new)
                .collect(Collectors.toList());
    }

    private ServiceCreator createServiceCreatorForRemote(String remoteUrl) {
        return ServiceCreator.withPayloadLimiter(
                metricsManager,
                // note that this refreshable never updates, even if the cluster configuration is mutable
                Refreshable.only(getServerListConfig(remoteUrl)),
                userAgent,
                () -> RemotingClientConfigs.DEFAULT);
    }

    private ServerListConfig getServerListConfig(String remoteUrl) {
        return ImmutableServerListConfig.builder()
                .addServers(PaxosRemotingUtils.addProtocol(cluster, remoteUrl))
                .sslConfiguration(cluster.cluster().security())
                .proxyConfiguration(cluster.cluster().proxyConfiguration())
                .build();
    }
}
