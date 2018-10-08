/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.util;

import java.nio.file.Paths;

import com.google.common.collect.ImmutableList;
import com.palantir.remoting.api.config.ssl.SslConfiguration;
import com.palantir.remoting3.clients.ClientConfiguration;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.config.ssl.SslSocketFactories;
import com.palantir.remoting3.jaxrs.JaxRsClient;

import io.dropwizard.testing.ResourceHelpers;

public final class TestJaxRsClientFactory {
    private TestJaxRsClientFactory() {
        // utility
    }

    public static <S, T> S createJaxRsClientForTest(
            Class<S> serviceClass,
            Class<T> testClass,
            String... uris) {
        return JaxRsClient.create(
                serviceClass,
                testClass.getName() + " (unknown)",
                createTestConfig(uris));
    }

    private static ClientConfiguration createTestConfig(String... uris) {
        SslConfiguration sslConfiguration = SslConfiguration.of(
                Paths.get(ResourceHelpers.resourceFilePath("trustStore.jks")));
        return ClientConfigurations.of(
                ImmutableList.copyOf(uris),
                SslSocketFactories.createSslSocketFactory(sslConfiguration),
                SslSocketFactories.createX509TrustManager(sslConfiguration));
    }
}
