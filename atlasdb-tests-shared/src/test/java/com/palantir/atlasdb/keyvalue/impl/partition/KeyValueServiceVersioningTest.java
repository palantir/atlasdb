package com.palantir.atlasdb.keyvalue.impl.partition;

import javax.annotation.Nullable;

import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.VersionedKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.remoting.InsufficientConsistencyExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.KeyAlreadyExistsExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.server.InboxPopulatingContainerRequestFilter;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class KeyValueServiceVersioningTest {

    private final SimpleModule module = RemotingKeyValueService.kvsModule();
    private final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    private final QuorumParameters parameters = new QuorumParameters(3, 2, 2);

    final KeyValueService remoteEndpoint = RemotingKeyValueService.createServerSide(new InMemoryKeyValueService(false), new Supplier<Long>() {
        @Override
        public Long get() {
            return 124L;
        }
    });

    @Rule
    public final DropwizardClientRule remoteEndpointService = new DropwizardClientRule(
            remoteEndpoint,
            KeyAlreadyExistsExceptionMapper.instance(),
            InsufficientConsistencyExceptionMapper.instance(),
            new InboxPopulatingContainerRequestFilter(mapper));

    @Test
    public void testSimple() {
        KeyValueService localEndpoint = RemotingKeyValueService.createClientSide(remoteEndpointService.baseUri().toString());
        KeyValueEndpoint kve = VersionedKeyValueEndpoint.create(null, Suppliers.<Long>ofInstance(123L), localEndpoint);

        kve.run(new Function<KeyValueService, Void>() {
            @Override @Nullable
            public Void apply(@Nullable KeyValueService input) {
                input.getAllTableNames();
                return null;
            }
        });
    }

}
