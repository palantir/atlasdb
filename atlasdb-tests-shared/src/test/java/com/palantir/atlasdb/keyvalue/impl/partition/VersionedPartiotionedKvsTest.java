package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.dropwizard.testing.junit.DropwizardClientRule;

import java.util.Map;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.remoting.Utils;
import com.palantir.atlasdb.keyvalue.remoting.Utils.RemoteKvs;
import com.palantir.atlasdb.keyvalue.remoting.Utils.RemotePms;

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

    RemotePms pms1 = new RemotePms();
    RemotePms pms2 = new RemotePms();
    RemotePms pms3 = new RemotePms();
    RemotePms pms4 = new RemotePms();

    RemoteKvs kvs1 = new RemoteKvs(pms1);
    RemoteKvs kvs2 = new RemoteKvs(pms2);
    RemoteKvs kvs3 = new RemoteKvs(pms3);
    RemoteKvs kvs4 = new RemoteKvs(pms4);

    SimpleKeyValueEndpoint kve1;
    SimpleKeyValueEndpoint kve2;
    SimpleKeyValueEndpoint kve3;
    SimpleKeyValueEndpoint kve4;

    NavigableMap<byte[], KeyValueEndpoint> ring;
    DynamicPartitionMapImpl pmap;
    PartitionedKeyValueService pkvs;

    @Rule public DropwizardClientRule kvsRule1 = kvs1.rule;
    @Rule public DropwizardClientRule kvsRule2 = kvs2.rule;
    @Rule public DropwizardClientRule kvsRule3 = kvs3.rule;
    @Rule public DropwizardClientRule kvsRule4 = kvs4.rule;
    @Rule public DropwizardClientRule pmsRule1 = pms1.rule;
    @Rule public DropwizardClientRule pmsRule2 = pms2.rule;
    @Rule public DropwizardClientRule pmsRule3 = pms3.rule;
    @Rule public DropwizardClientRule pmsRule4 = pms4.rule;

    public void setUpPrivate() {
        kve1 = new SimpleKeyValueEndpoint(kvs1.rule.baseUri().toString(), pms1.rule.baseUri().toString());
        kve2 = new SimpleKeyValueEndpoint(kvs2.rule.baseUri().toString(), pms2.rule.baseUri().toString());
        kve3 = new SimpleKeyValueEndpoint(kvs3.rule.baseUri().toString(), pms3.rule.baseUri().toString());
        kve4 = new SimpleKeyValueEndpoint(kvs4.rule.baseUri().toString(), pms4.rule.baseUri().toString());

        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        ring.put(new byte[] {0},       kve1);
        ring.put(new byte[] {0, 0},    kve2);
        ring.put(new byte[] {0, 0, 0}, kve3);
        // Do not insert kve4 - it will be used later to test addEndpoint

        pmap = DynamicPartitionMapImpl.create(ring);
        pkvs = PartitionedKeyValueService.create(new QuorumParameters(3, 3, 3), pmap);

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
        Utils.setupRuleHacks(kvsRule4);
        Utils.setupRuleHacks(pmsRule1);
        Utils.setupRuleHacks(pmsRule2);
        Utils.setupRuleHacks(pmsRule3);
        Utils.setupRuleHacks(kvsRule4);
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

    @Test
    public void testAddEndpoint() {
        Map<Cell, Value> emptyResult = ImmutableMap.<Cell, Value>of();

        kve4.partitionMapService().update(pkvs.getPartitionMap());
        pkvs.getPartitionMap().addEndpoint(new byte[] {(byte)0xff, 0, 0, 0}, kve4, "", false);
        pkvs.getPartitionMap().syncAddEndpoint();
        kve4.partitionMapService().update(pkvs.getPartitionMap());
        kve1.partitionMapService().update(pkvs.getPartitionMap());
        kve2.partitionMapService().update(pkvs.getPartitionMap());
        kve3.partitionMapService().update(pkvs.getPartitionMap());

        Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        // Force pmap update (why not)
        pkvs.getPartitionMap().setVersion(0L);
        try {
            pkvs.createTable(TEST_TABLE, 12345);
            fail();
        } catch (VersionTooOldException e) {
            pkvs.createTable(TEST_TABLE, 12345);
        }
        assertEquals(1L, pkvs.getPartitionMap().getVersion());
        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);

        assertEquals(result0, kvs1.inMemoryKvs.get(TEST_TABLE, cells0));
        assertEquals(result0, kvs2.inMemoryKvs.get(TEST_TABLE, cells0));
        assertEquals(result0, kvs3.inMemoryKvs.get(TEST_TABLE, cells0));
        assertEquals(result0, kvs4.inMemoryKvs.get(TEST_TABLE, cells0));

        pkvs.getPartitionMap().finalizeAddEndpoint(new byte[] {(byte)0xff, 0, 0, 0});
        kve4.partitionMapService().update(pkvs.getPartitionMap());
        kve1.partitionMapService().update(pkvs.getPartitionMap());
        kve2.partitionMapService().update(pkvs.getPartitionMap());
        kve3.partitionMapService().update(pkvs.getPartitionMap());

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        // This time without a forced update
        assertEquals(2L, pkvs.getPartitionMap().getVersion());
        assertEquals(2L, kve1.partitionMapService().getVersion());
        assertEquals(2L, kve2.partitionMapService().getVersion());
        assertEquals(2L, kve3.partitionMapService().getVersion());
        assertEquals(2L, kve4.partitionMapService().getVersion());
        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        assertEquals(result1, kvs1.inMemoryKvs.get(TEST_TABLE, cells1));
        assertEquals(result1, kvs2.inMemoryKvs.get(TEST_TABLE, cells1));
        assertEquals(emptyResult, kvs3.inMemoryKvs.get(TEST_TABLE, cells1));
        assertEquals(result1, kvs4.inMemoryKvs.get(TEST_TABLE, cells1));
    }

    @Test
    public void testRemoveEndpoint() {

        Map<Cell, Value> emptyResult = ImmutableMap.<Cell, Value>of();

        // First add the endpoint so that we can remove one
        kve4.partitionMapService().update(pkvs.getPartitionMap());

        pkvs.getPartitionMap().addEndpoint(new byte[] {(byte)0xff, 0, 0, 0}, kve4, "", false);
        pkvs.getPartitionMap().syncAddEndpoint();
        kve4.partitionMapService().update(pkvs.getPartitionMap());
        kve1.partitionMapService().update(pkvs.getPartitionMap());
        kve2.partitionMapService().update(pkvs.getPartitionMap());
        kve3.partitionMapService().update(pkvs.getPartitionMap());

        pkvs.getPartitionMap().finalizeAddEndpoint(new byte[] {(byte)0xff, 0, 0, 0});
        kve4.partitionMapService().update(pkvs.getPartitionMap());
        kve1.partitionMapService().update(pkvs.getPartitionMap());
        kve2.partitionMapService().update(pkvs.getPartitionMap());
        kve3.partitionMapService().update(pkvs.getPartitionMap());

        pkvs.createTable(TEST_TABLE, 12345);

        pkvs.getPartitionMap().removeEndpoint(new byte[] {0, 0}, false);
        pkvs.getPartitionMap().syncRemoveEndpoint();
        kve1.partitionMapService().update(pkvs.getPartitionMap());
        kve2.partitionMapService().update(pkvs.getPartitionMap());
        kve3.partitionMapService().update(pkvs.getPartitionMap());
        kve4.partitionMapService().update(pkvs.getPartitionMap());

        Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);
        assertEquals(result0, kvs1.inMemoryKvs.get(TEST_TABLE, cells0));
        assertEquals(result0, kvs2.inMemoryKvs.get(TEST_TABLE, cells0));
        assertEquals(result0, kvs3.inMemoryKvs.get(TEST_TABLE, cells0));
        assertEquals(result0, kvs4.inMemoryKvs.get(TEST_TABLE, cells0));

        pkvs.getPartitionMap().finalizeRemoveEndpoint(new byte[] {0, 0});
        kve1.partitionMapService().update(pkvs.getPartitionMap());
        kve3.partitionMapService().update(pkvs.getPartitionMap());
        kve4.partitionMapService().update(pkvs.getPartitionMap());

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        assertEquals(result1, kvs1.inMemoryKvs.get(TEST_TABLE, cells1));
        assertEquals(emptyResult, kvs2.inMemoryKvs.get(TEST_TABLE, cells1));
        assertEquals(result1, kvs3.inMemoryKvs.get(TEST_TABLE, cells1));
        assertEquals(result1, kvs4.inMemoryKvs.get(TEST_TABLE, cells1));
    }

    @Override
    protected KeyValueService getKeyValueService() {
        setUpPrivate();
        return Preconditions.checkNotNull(pkvs);
    }
}
