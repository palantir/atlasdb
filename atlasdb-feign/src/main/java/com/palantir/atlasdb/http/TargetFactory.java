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

package com.palantir.atlasdb.http;

import java.net.ProxySelector;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.TrustContext;

public interface TargetFactory {
    <T> T createProxyWithoutRetrying(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize);

    <T> T createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize);

    <T> T createProxyWithFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize);

    <T> T createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent,
            boolean limitPayload);

    String getClientVersion();
}
