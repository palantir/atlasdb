package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.DynamicPartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.Utils;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class VersionedPartiotionedKvsTest extends AbstractAtlasDbKeyValueServiceTest {

    /**
     * RemoteKVS - InMemoryKeyValueService          RemotePMS - PartitionMapServiceImpl
     * |                                            |
     * RemoteKVSService - DropwizardClientRule      RemotePMSService - DropwizardClientRule
     * |                                            |
     * LocalKVS - Feign                             LocalPMS - Feign
     * |                                            |
     * ----------------------------------------------
     * |
     * SimpleKeyValueEndpoint
     * |
     * DynamicPartitionMapImpl
     * |
     * PartitionedKeyValueService
     *
     *
     */

    static class RemoteKvs {
        final KeyValueService inMemoryKvs = new InMemoryKeyValueService(false);
        final KeyValueService remoteKvs;
        final DropwizardClientRule rule;

        public RemoteKvs(final RemotePms remotePms) {
            remoteKvs = RemotingKeyValueService.createServerSide(inMemoryKvs, new Supplier<Long>() {
                @Override
                public Long get() {
                    Long version = RemotingPartitionMapService.createClientSide(remotePms.rule.baseUri().toString()).getVersion();
                    return version;
                }
            });
            rule = Utils.getRemoteKvsRule(remoteKvs);
        }
    }

    static class RemotePms {
        PartitionMapService service = new PartitionMapServiceImpl();
        DropwizardClientRule rule = new DropwizardClientRule(service);
    }

    RemotePms pms1 = new RemotePms();
    RemotePms pms2 = new RemotePms();
    RemotePms pms3 = new RemotePms();

    RemoteKvs kvs1 = new RemoteKvs(pms1);
    RemoteKvs kvs2 = new RemoteKvs(pms2);
    RemoteKvs kvs3 = new RemoteKvs(pms3);

    SimpleKeyValueEndpoint kve1;
    SimpleKeyValueEndpoint kve2;
    SimpleKeyValueEndpoint kve3;

    NavigableMap<byte[], KeyValueEndpoint> ring;
    DynamicPartitionMap pmap;
    KeyValueService pkvs;

    @Rule public DropwizardClientRule kvsRule1 = kvs1.rule;
    @Rule public DropwizardClientRule kvsRule2 = kvs2.rule;
    @Rule public DropwizardClientRule kvsRule3 = kvs3.rule;
    @Rule public DropwizardClientRule pmsRule1 = pms1.rule;
    @Rule public DropwizardClientRule pmsRule2 = pms2.rule;
    @Rule public DropwizardClientRule pmsRule3 = pms3.rule;

    public void setUpPrivate() {
        kve1 = new SimpleKeyValueEndpoint(kvs1.rule.baseUri().toString(), pms1.rule.baseUri().toString());
        kve2 = new SimpleKeyValueEndpoint(kvs2.rule.baseUri().toString(), pms2.rule.baseUri().toString());
        kve3 = new SimpleKeyValueEndpoint(kvs3.rule.baseUri().toString(), pms3.rule.baseUri().toString());

        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        ring.put(new byte[] {0},       kve1);
        ring.put(new byte[] {0, 0},    kve2);
        ring.put(new byte[] {0, 0, 0}, kve3);

        pmap = DynamicPartitionMapImpl.create(ring);
        pkvs = DynamicPartitionedKeyValueService.create(pmap);
    }

    @Before
    public void setupHacks() {
        Utils.setupRuleHacks(kvsRule1);
        Utils.setupRuleHacks(kvsRule2);
        Utils.setupRuleHacks(kvsRule3);
        Utils.setupRuleHacks(pmsRule1);
        Utils.setupRuleHacks(pmsRule2);
        Utils.setupRuleHacks(pmsRule3);
    }

    @Test
    public void testSetup() {
    }

    @Override
    protected KeyValueService getKeyValueService() {
        setUpPrivate();
        kve1.partitionMapService().update(1, pmap);
        kve1.partitionMapService().get();
//        kve1.partitionMapService().update(1, 2L);
//        kve2.partitionMapService().update(2, pmap);
//        kve3.partitionMapService().update(3, pmap);
        return Preconditions.checkNotNull(pkvs);
    }
}
