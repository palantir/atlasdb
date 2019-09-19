/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public abstract class TimelockProxyFactories {

    abstract TimeLockInstallConfiguration install();
    abstract MetricRegistry metrics();

    <T> List<T> createRemoteProxies(Class<T> clazz, String userAgent) {
        Set<String> remoteUris = PaxosRemotingUtils.getRemoteServerPaths(install());
        Optional<TrustContext> trustContext = PaxosRemotingUtils.getSslConfigurationOptional(install())
                .map(SslSocketFactories::createTrustContext);
        return remoteUris.stream()
                .map(uri -> AtlasDbHttpClients.createProxy(
                        metrics(),
                        trustContext,
                        uri,
                        clazz,
                        userAgent,
                        false))
                .collect(Collectors.toList());
    }

    <T> LocalAndRemotes<T> instrumentLocalAndRemotesFor(Class<T> clazz, T local, List<T> remotes) {
        return LocalAndRemotes.of(local, remotes)
                .map(instance -> ServiceCreator.createInstrumentedService(metrics(), instance, clazz));
    }

}
