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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableServerListConfig.class)
@JsonSerialize(as = ImmutableServerListConfig.class)
@Value.Immutable
public interface ServerListConfig {
    Set<String> servers();

    Optional<SslConfiguration> sslConfiguration();

    Optional<ProxyConfiguration> proxyConfiguration();

    @Value.Lazy
    default Optional<TrustContext> trustContext() {
        return sslConfiguration().map(SslSocketFactories::createTrustContext);
    }

    default boolean hasAtLeastOneServer() {
        return servers().size() >= 1;
    }
}
