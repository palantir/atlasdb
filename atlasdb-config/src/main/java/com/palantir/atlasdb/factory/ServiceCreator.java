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
package com.palantir.atlasdb.factory;

import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting2.config.ssl.SslConfiguration;
import com.palantir.remoting2.config.ssl.SslSocketFactories;

public class ServiceCreator<T> implements Function<ServerListConfig, T> {
    private final Class<T> serviceClass;
    private final String userAgent;

    public ServiceCreator(Class<T> serviceClass, String userAgent) {
        this.serviceClass = serviceClass;
        this.userAgent = userAgent;
    }

    @Override
    public T apply(ServerListConfig input) {
        Optional<SSLSocketFactory> sslSocketFactory = createSslSocketFactory(input.sslConfiguration());
        return createService(sslSocketFactory, input.servers(), serviceClass, userAgent);
    }

    /**
     * Utility method for transforming an optional {@link SslConfiguration} into an optional {@link SSLSocketFactory}.
     */
    public static Optional<SSLSocketFactory> createSslSocketFactory(Optional<SslConfiguration> sslConfiguration) {
        return sslConfiguration.transform(config -> SslSocketFactories.createSslSocketFactory(config));
    }

    public static <T> T createService(
            Optional<SSLSocketFactory> sslSocketFactory,
            Set<String> uris,
            Class<T> serviceClass,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                serviceClass,
                AtlasDbHttpClients.createProxyWithFailover(sslSocketFactory, uris, serviceClass, userAgent),
                MetricRegistry.name(serviceClass, userAgent));
    }

}
