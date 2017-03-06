/**
 * Copyright 2017 Palantir Technologies
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

import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.remoting.ssl.SslSocketFactories;
import com.palantir.tritium.event.log.LoggingInvocationEventHandler;
import com.palantir.tritium.event.log.LoggingLevel;
import com.palantir.tritium.event.metrics.MetricsInvocationEventHandler;
import com.palantir.tritium.proxy.Instrumentation;

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
        return instrument(userAgent, serviceClass,
                AtlasDbHttpClients.createProxyWithFailover(sslSocketFactory, uris, serviceClass, userAgent));
    }

    private static <T> T instrument(String userAgent, Class<T> serviceClass, T remoteService) {
        String name = MetricRegistry.name(serviceClass, userAgent);
        return Instrumentation.builder(serviceClass, remoteService)
                .withHandler(new MetricsInvocationEventHandler(AtlasDbMetrics.getMetricRegistry(), name))
                .withLogging(
                        LoggerFactory.getLogger("performance." + name),
                        LoggingLevel.TRACE,
                        LoggingInvocationEventHandler.LOG_DURATIONS_GREATER_THAN_1_MICROSECOND)
                .build();
    }
}
