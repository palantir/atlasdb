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

package com.palantir.atlasdb.http.v2;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.ImmutableInstanceAndVersion;
import com.palantir.atlasdb.http.TargetFactory;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.okhttp.NoOpHostEventsSink;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.function.Supplier;

public final class ConjureJavaRuntimeTargetFactory implements TargetFactory {

    public static final ConjureJavaRuntimeTargetFactory DEFAULT = new ConjureJavaRuntimeTargetFactory();
    public static final String CLIENT_VERSION_STRING = "Conjure-Java-Runtime";

    private ConjureJavaRuntimeTargetFactory() {
        // Use the instances.
    }

    @Override
    public <T> InstanceAndVersion<T> createProxy(
            Optional<TrustContext> trustContext, String uri, Class<T> type, AuxiliaryRemotingParameters parameters) {
        ClientOptions relevantOptions = ClientOptions.fromRemotingParameters(parameters);
        ClientConfiguration clientConfiguration = relevantOptions.create(
                ImmutableList.of(uri),
                Optional.empty(),
                trustContext.orElseThrow(() -> new IllegalStateException("CJR requires a trust context")));
        T client = JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                NoOpHostEventsSink.INSTANCE,
                clientConfiguration);
        return wrapWithVersion(client);
    }

    @Override
    public <T> InstanceAndVersion<T> createProxyWithFailover(
            ServerListConfig serverListConfig, Class<T> type, AuxiliaryRemotingParameters parameters) {
        // It doesn't make sense to create a proxy with the capacity to failover that doesn't retry.
        ClientOptions clientOptions = getClientOptionsForFailoverProxy(parameters);
        return createFailoverProxy(serverListConfig, type, parameters, clientOptions);
    }

    @Override
    public <T> InstanceAndVersion<T> createLiveReloadingProxyWithFailover(
            Refreshable<ServerListConfig> serverListConfigRefreshable,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        ClientOptions options = getClientOptionsForFailoverProxy(parameters);
        Refreshable<T> refreshableClient = serverListConfigRefreshable.map(serverListConfig -> JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                NoOpHostEventsSink.INSTANCE,
                options.serverListToClient(serverListConfig)));
        return decorateFailoverProxy(type, refreshableClient);
    }

    private <T> InstanceAndVersion<T> createFailoverProxy(
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters,
            ClientOptions clientOptions) {
        ClientConfiguration clientConfiguration = clientOptions.serverListToClient(serverListConfig);

        T client = JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                NoOpHostEventsSink.INSTANCE,
                clientConfiguration);
        return decorateFailoverProxy(type, () -> client);
    }

    private static <T> InstanceAndVersion<T> decorateFailoverProxy(Class<T> type, Supplier<T> client) {
        return wrapWithVersion(FastFailoverProxy.newProxyInstance(type, client));
    }

    private static UserAgent addAtlasDbRemotingAgent(UserAgent agent) {
        return agent.addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
    }

    private static <T> InstanceAndVersion<T> wrapWithVersion(T instance) {
        return ImmutableInstanceAndVersion.of(instance, CLIENT_VERSION_STRING);
    }

    private static ClientOptions getClientOptionsForFailoverProxy(AuxiliaryRemotingParameters parameters) {
        return ClientOptions.fromRemotingParameters(
                ImmutableAuxiliaryRemotingParameters.copyOf(parameters).withShouldRetry(true));
    }
}
