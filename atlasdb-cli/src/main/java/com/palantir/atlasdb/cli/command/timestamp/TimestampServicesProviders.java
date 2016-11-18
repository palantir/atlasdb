/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.cli.command.timestamp;

import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.timestamp.TimestampAdminService;
import com.palantir.timestamp.TimestampService;

public final class TimestampServicesProviders {
    private static final Map<AtlasDbConfig, TimestampServicesProvider> PROVIDER_MAP = Maps.newHashMap();

    private TimestampServicesProviders() {
        // utility class
    }

    public static synchronized TimestampServicesProvider getInternalProviderFromAtlasDbConfig(AtlasDbConfig config) {
        TimestampServicesProvider provider = PROVIDER_MAP.get(config);
        if (provider == null) {
            provider = createInternalProviderFromAtlasDbConfig(config);
            PROVIDER_MAP.put(config, provider);
        }
        return provider;
    }

    private static TimestampServicesProvider createInternalProviderFromAtlasDbConfig(AtlasDbConfig config) {
        ServicesConfigModule scm = ServicesConfigModule.create(
                ImmutableAtlasDbConfig.copyOf(config)
                        .withTimelock(Optional.absent()));
        TimestampService service = DaggerAtlasDbServices.builder()
                .servicesConfigModule(scm)
                .build()
                .getTimestampService();
        return createFromSingleService(service);
    }

    public static TimestampServicesProvider createFromSingleService(TimestampService service) {
        if (!(service instanceof TimestampAdminService)) {
            throw new IllegalStateException("Timestamp service must also have administrative capabilities.");
        }
        return ImmutableTimestampServicesProvider.builder()
                .timestampService(service)
                .timestampAdminService((TimestampAdminService) service)
                .build();
    }

    public static TimestampServicesProvider createFromTimelockConfiguration(TimeLockClientConfig config) {
        return ImmutableTimestampServicesProvider.builder()
                .timestampService(getTimelockProxy(config, TimestampService.class))
                .timestampAdminService(getTimelockProxy(config, TimestampAdminService.class))
                .build();
    }

    private static <T> T getTimelockProxy(TimeLockClientConfig timeLockClientConfig, Class<T> clazz) {
        return AtlasDbHttpClients.createProxyWithFailover(
                createSslSocketFactoryFromClientConfig(timeLockClientConfig),
                timeLockClientConfig.serverListConfig()
                        .servers()
                        .stream()
                        .map(server -> server + "/" + timeLockClientConfig.client())
                        .collect(Collectors.toList()),
                clazz);
    }

    private static Optional<SSLSocketFactory> createSslSocketFactoryFromClientConfig(TimeLockClientConfig config) {
        return TransactionManagers.createSslSocketFactory(config.serverListConfig().sslConfiguration());
    }
}
