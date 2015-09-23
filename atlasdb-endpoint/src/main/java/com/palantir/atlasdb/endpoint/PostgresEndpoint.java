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
package com.palantir.atlasdb.endpoint;

import java.lang.reflect.Proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.InKvsPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.server.EndpointServer;
import com.palantir.atlasdb.keyvalue.rdbms.PostgresKeyValueConfiguration;
import com.palantir.atlasdb.keyvalue.rdbms.PostgresKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.ClientVersionTooOldExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.EndpointVersionTooOldExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.InsufficientConsistencyExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.KeyAlreadyExistsExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.outofband.InboxPopulatingContainerRequestFilter;
import com.palantir.common.proxy.AbstractDelegatingInvocationHandler;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * This class is to be run as a stand-alone endpoint
 * for <tt>PartitionedKeyValueService</tt>.
 *
 * Data as well as partition map are stored in a
 * Postgres database based on the provided config file.
 *
 * @author htarasiuk
 *
 */
public class PostgresEndpoint extends Application<EndpointServerConfiguration> {

    @SuppressWarnings("unchecked")
    static <T> T identityProxy(final T delegate, Class<T> delegateClass) {
        return (T) Proxy.newProxyInstance(
                delegateClass.getClassLoader(),
                new Class<?>[] { delegateClass },
                new AbstractDelegatingInvocationHandler() {
            @Override
            public Object getDelegate() {
                return delegate;
            }
        });
    }

    private final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    public static void main(String[] args) throws Exception {
        new PostgresEndpoint().run(args);
    }

    private PostgresEndpoint() {
    }

    @Override
    public void run(EndpointServerConfiguration configuration,
            Environment environment) throws Exception {
        PostgresKeyValueConfiguration config = mapper.treeToValue(configuration.extraConfig.get("postgresConfig"), PostgresKeyValueConfiguration.class);
        PostgresKeyValueService kvs = PostgresKeyValueService.create(config);
        kvs.initializeFromFreshInstance();
        final InKvsPartitionMapService pms = InKvsPartitionMapService.create(kvs);

        final EndpointServer server = new EndpointServer(RemotingKeyValueService.createServerSide(kvs, new Supplier<Long>() {
            @Override
            public Long get() {
                return pms.getMapVersion();
            }
        }), pms);

        // Wrap server with two proxies.
        // Otherwise Jersey will not handle properly a single
        // object that implements two annotated interfaces.
        KeyValueService kvsProxy = identityProxy(server, KeyValueService.class);
        PartitionMapService pmsProxy = identityProxy(server, PartitionMapService.class);

        environment.jersey().register(new InboxPopulatingContainerRequestFilter(mapper));
        environment.jersey().register(KeyAlreadyExistsExceptionMapper.instance());
        environment.jersey().register(InsufficientConsistencyExceptionMapper.instance());
        environment.jersey().register(ClientVersionTooOldExceptionMapper.instance());
        environment.jersey().register(EndpointVersionTooOldExceptionMapper.instance());
        environment.getObjectMapper().registerModule(RemotingKeyValueService.kvsModule());
        environment.getObjectMapper().registerModule(new GuavaModule());
        environment.jersey().register(kvsProxy);
        environment.jersey().register(pmsProxy);
    }

}
