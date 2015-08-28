package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
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
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.Utils;
import com.palantir.util.Pair;

import io.dropwizard.testing.junit.DropwizardClientRule;

/**
 * This test is to make sure that out of date exceptions are handled in a proper way.
 * 
 * @author htarasiuk
 *
 */
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
    
    static DynamicPartitionMap createNewMap(Collection<Pair<RemoteKvs, RemotePms>> endpoints) {
    	ArrayList<Byte> keyList = new ArrayList<>();
    	NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
    	keyList.add((byte) 0);
    	for (Pair<RemoteKvs, RemotePms> p : endpoints) {
    		SimpleKeyValueEndpoint kvs = new SimpleKeyValueEndpoint(p.lhSide.rule.baseUri().toString(), p.rhSide.rule.baseUri().toString());
    		byte[] key = ArrayUtils.toPrimitive(keyList.toArray(new Byte[keyList.size()]));
    		ring.put(key, kvs);
    	}
    	return DynamicPartitionMapImpl.create(ring);
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
        pkvs = PartitionedKeyValueService.create(pmap);

        // Push the map to all the endpoints
        kve1.partitionMapService().update(pmap);
        kve2.partitionMapService().update(pmap);
        kve3.partitionMapService().update(pmap);
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
    public void testVersionTooOld() {
    	pmap.setVersion(1L);
    	kve1.partitionMapService().update(pmap);
    	pmap.setVersion(0L);
    	assertEquals(1L, kve1.partitionMapService().getVersion());
    	try {
    		pkvs.createTable("TABLE_NAME_2", 12345);
    		// This has to throw since table metadata is to be
    		// stored on all endpoints.
    		fail();
    	} catch (VersionTooOldException e) {
    		pkvs.createTable("TABLE_NAME_2", 12345);
    		assertTrue(pkvs.getAllTableNames().contains("TABLE_NAME_2"));
    	}
    }

    @Override
    protected KeyValueService getKeyValueService() {
        setUpPrivate();
        return Preconditions.checkNotNull(pkvs);
    }
}
