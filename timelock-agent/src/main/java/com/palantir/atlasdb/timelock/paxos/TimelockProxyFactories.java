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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;

@Value.Immutable
public abstract class TimelockProxyFactories {

    abstract TimeLockInstallConfiguration install();
    abstract TimelockPaxosMetrics metrics();

    <T> List<T> createInstrumentedRemoteProxies(Class<T> clazz, String name) {
        Set<String> remoteUris = PaxosRemotingUtils.getRemoteServerPaths(install());
        Optional<TrustContext> trustContext = PaxosRemotingUtils
                .getSslConfigurationOptional(install())
                .map(SslSocketFactories::createTrustContext);

        AuxiliaryRemotingParameters parameters = AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(false)
                .userAgent(UserAgent.of(UserAgent.Agent.of(name, UserAgent.Agent.DEFAULT_VERSION)))
                .build();

        return remoteUris.stream()
                .map(uri -> AtlasDbHttpClients.DEFAULT_TARGET_FACTORY.createProxy(
                        trustContext,
                        uri,
                        clazz,
                        parameters))
                .map(proxy -> metrics().instrument(clazz, proxy, name))
                .collect(Collectors.toList());
    }

    <T> LocalAndRemotes<T> instrumentLocalAndRemotesFor(Class<T> clazz, T local, List<T> remotes, String name) {
        return LocalAndRemotes.of(local, remotes)
                .map(instance -> metrics().instrument(clazz, instance, name));
    }

    <T> LocalAndRemotes<T> instrumentLocalAndRemotesFor(
            Class<T> clazz,
            T local,
            List<T> remotes,
            String name,
            Client client) {
        return LocalAndRemotes.of(local, remotes)
                .map(instance -> metrics().instrument(clazz, instance, name, client));
    }

}
