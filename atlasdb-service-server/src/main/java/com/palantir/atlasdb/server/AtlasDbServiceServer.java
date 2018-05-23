/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.server;

import com.codahale.metrics.SharedMetricRegistries;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.impl.AtlasDbServiceImpl;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.jackson.AtlasJacksonModule;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbServiceServer extends Application<AtlasDbServiceServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new AtlasDbServiceServer().run(args);
    }

    @Override
    public void initialize(Bootstrap<AtlasDbServiceServerConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setMetricRegistry(SharedMetricRegistries.getOrCreate("AtlasDbTest"));
    }

    @Override
    public void run(AtlasDbServiceServerConfiguration config, final Environment environment) throws Exception {
        AtlasDbMetrics.setMetricRegistries(environment.metrics(), DefaultTaggedMetricRegistry.getDefault());

        TransactionManager tm = TransactionManagers.builder()
                .config(config.getConfig())
                .userAgent("AtlasDbServiceServer")
                .globalMetricsRegistry(environment.metrics())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment.jersey()::register)
                .build()
                .serializable();

        TableMetadataCache cache = new TableMetadataCache(tm.getKeyValueService());

        environment.jersey().register(new AtlasDbServiceImpl(tm.getKeyValueService(), tm, cache));
        environment.getObjectMapper().registerModule(new AtlasJacksonModule(cache).createModule());
    }

}
