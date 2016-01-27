package com.palantir.atlasdb.keyvalue.jdbc;

import java.lang.reflect.Proxy;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.InKvsPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.server.EndpointServer;
import com.palantir.atlasdb.keyvalue.remoting.ClientVersionTooOldExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.EndpointVersionTooOldExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.InsufficientConsistencyExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.KeyAlreadyExistsExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.outofband.InboxPopulatingContainerRequestFilter;
import com.palantir.atlasdb.spi.AtlasDbServerEnvironment;
import com.palantir.atlasdb.spi.AtlasDbServicePlugin;
import com.palantir.common.proxy.AbstractDelegatingInvocationHandler;

@JsonDeserialize(as = ImmutableJdbcServicePlugin.class)
@JsonSerialize(as = ImmutableJdbcServicePlugin.class)
@JsonTypeName(JdbcKeyValueConfiguration.TYPE)
@Value.Immutable
public abstract class JdbcServicePlugin implements AtlasDbServicePlugin {
    @Override
    public String type() {
        return JdbcKeyValueConfiguration.TYPE;
    }

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


    @Override
    public void registerServices(AtlasDbServerEnvironment environment) {
        KeyValueService kvs = new JdbcAtlasDbFactory().createRawKeyValueService(getJdbcConfig());
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

        environment.register(new InboxPopulatingContainerRequestFilter(mapper));
        environment.register(KeyAlreadyExistsExceptionMapper.instance());
        environment.register(InsufficientConsistencyExceptionMapper.instance());
        environment.register(ClientVersionTooOldExceptionMapper.instance());
        environment.register(EndpointVersionTooOldExceptionMapper.instance());
        environment.getObjectMapper().registerModule(RemotingKeyValueService.kvsModule());
        environment.getObjectMapper().registerModule(new GuavaModule());
        environment.register(kvsProxy);
        environment.register(pmsProxy);
    }

    public abstract JdbcKeyValueConfiguration getJdbcConfig();

}
