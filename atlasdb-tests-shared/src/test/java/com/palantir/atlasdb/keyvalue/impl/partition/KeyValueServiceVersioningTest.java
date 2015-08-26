package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.VersionedKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.VersionedKeyValueEndpoint.UpdatePartitionMapException;
import com.palantir.atlasdb.keyvalue.partition.VersionedPartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.remoting.InsufficientConsistencyExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.KeyAlreadyExistsExceptionMapper;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.VersionTooOldExceptionMapper;
import com.palantir.atlasdb.server.InboxPopulatingContainerRequestFilter;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class KeyValueServiceVersioningTest {

    private final SimpleModule module = RemotingKeyValueService.kvsModule();
    private final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    private final QuorumParameters parameters = new QuorumParameters(3, 2, 2);
    private final Mutable<Long> serverVersion = Mutables.newMutable();
    private final Mutable<Long> clientVersion = Mutables.newMutable();

    private final Supplier<Long> serverVersionSupplier = new Supplier<Long>() {
        @Override
        public Long get() {
            return serverVersion.get();
        }
    };

    private final Supplier<Long> clientVersionSupplier = new Supplier<Long>() {
        @Override
        public Long get() {
            return clientVersion.get();
        }
    };

    final KeyValueService remoteEndpoint = RemotingKeyValueService.createServerSide(new InMemoryKeyValueService(false), serverVersionSupplier);
    final KeyValueEndpoint whatever = VersionedKeyValueEndpoint.fake();
    final ImmutableSortedMap<byte[], KeyValueEndpoint> ring =
            ImmutableSortedMap
                    .<byte[], KeyValueEndpoint>orderedBy(UnsignedBytes.lexicographicalComparator())
                    .put(new byte[] {0}, whatever)
                    .put(new byte[] {0, 0}, whatever)
                    .put(new byte[] {0, 0, 0}, whatever)
                    .build();
    final PartitionMap pm = new DynamicPartitionMapImpl(parameters, ring);
    final PartitionMapService pms = new PartitionMapServiceImpl(pm, 0);

    @Rule
    public final DropwizardClientRule remoteEndpointService = new DropwizardClientRule(
            remoteEndpoint,
            KeyAlreadyExistsExceptionMapper.instance(),
            InsufficientConsistencyExceptionMapper.instance(),
            VersionTooOldExceptionMapper.instance(),
            new InboxPopulatingContainerRequestFilter(mapper));

    KeyValueService localEndpoint;
    KeyValueEndpoint kve;
    VersionedPartitionedKeyValueService vpkvs;

    @Before
    public void setUp() {
        localEndpoint = RemotingKeyValueService.createClientSide(remoteEndpointService.baseUri().toString());
        vpkvs = VersionedPartitionedKeyValueService.create(ring, PTExecutors.newCachedThreadPool(), parameters);
        PartitionMapService pms = vpkvs.getVersionedPartitionMap().getPartitionMapService();
        kve = VersionedKeyValueEndpoint.create(pms, clientVersionSupplier, localEndpoint);
    }

    @Test
    public void testPartitionMapOutOfDate() {

        clientVersion.set(123L);
        serverVersion.set(123L);
        kve.run(new Function<KeyValueService, Void>() {
            @Override @Nullable
            public Void apply(@Nullable KeyValueService input) {
                input.getAllTableNames();
                return null;
            }
        });

        serverVersion.set(124L);
        try {
            kve.run(new Function<KeyValueService, Void>() {
                @Override @Nullable
                public Void apply(@Nullable KeyValueService input) {
                    input.getAllTableNames();
                    return null;
                }
            });
            Assert.fail();
        } catch (UpdatePartitionMapException e) {
            assertEquals(124L, e.getNewVersion());
        }
    }

}
