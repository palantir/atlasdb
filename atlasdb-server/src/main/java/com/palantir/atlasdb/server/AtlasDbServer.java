/**
 * Copyright 2015 Palantir Technologies
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

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.impl.AtlasDbServiceImpl;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.jackson.AtlasJacksonModule;
import com.palantir.atlasdb.spi.AtlasDbServerEnvironment;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class AtlasDbServer extends Application<AtlasDbServerConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbServer.class);

    public static void main(String[] args) throws Exception {
        try {
            new AtlasDbServer().run(args);
        } catch (Throwable t) {
            log.error("failed to start server", t);
            System.exit(1);
        }
    }

    @Override
    public void run(AtlasDbServerConfiguration config, final Environment environment) throws Exception {
        SerializableTransactionManager tm = TransactionManagers.create(config.getConfig(), Optional.<SSLSocketFactory>absent(), ImmutableSet.<Schema>of(),
                new AtlasDbServerEnvironment() {
                    @Override
                    public void register(Object resource) {
                        environment.jersey().register(resource);
                    }
                    @Override
                    public ObjectMapper getObjectMapper() {
                        return environment.getObjectMapper();
                    }
                });

        TableMetadataCache cache = new TableMetadataCache(tm.getKeyValueService());

        environment.jersey().register(new AtlasDbServiceImpl(tm.getKeyValueService(), tm, cache));
        environment.getObjectMapper().registerModule(new AtlasJacksonModule(cache).createModule());
    }

}
