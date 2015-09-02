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
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.remoting.Utils;
import com.palantir.atlasdb.keyvalue.remoting.Utils.RemoteEndpoint;

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

    private static int NUM_EPTS = 4;
    RemoteEndpoint[] epts = new RemoteEndpoint[NUM_EPTS]; {
        for (int i=0; i<NUM_EPTS; ++i) {
            epts[i] = new RemoteEndpoint(new InMemoryKeyValueService(false), new PartitionMapServiceImpl());
        }
    };

    SimpleKeyValueEndpoint[] skves = new SimpleKeyValueEndpoint[NUM_EPTS];

    NavigableMap<byte[], KeyValueEndpoint> ring;
    DynamicPartitionMapImpl pmap;
    PartitionedKeyValueService pkvs;

    @Rule public DropwizardClientRule kvsRule1 = epts[0].kvs.rule;
    @Rule public DropwizardClientRule kvsRule2 = epts[1].kvs.rule;
    @Rule public DropwizardClientRule kvsRule3 = epts[2].kvs.rule;
    @Rule public DropwizardClientRule kvsRule4 = epts[3].kvs.rule;
    @Rule public DropwizardClientRule pmsRule1 = epts[0].pms.rule;
    @Rule public DropwizardClientRule pmsRule2 = epts[1].pms.rule;
    @Rule public DropwizardClientRule pmsRule3 = epts[2].pms.rule;
    @Rule public DropwizardClientRule pmsRule4 = epts[3].pms.rule;

    public void setUpPrivate() {
        for (int i=0; i<NUM_EPTS; ++i) {
            skves[i] = new SimpleKeyValueEndpoint(epts[i].kvs.rule.baseUri().toString(), epts[i].pms.rule.baseUri().toString());
        }

        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        ring.put(new byte[] {0},       skves[0]);
        ring.put(new byte[] {0, 0},    skves[1]);
        ring.put(new byte[] {0, 0, 0}, skves[2]);
        // Do not insert skves[3] - it will be used later to test addEndpoint

        pmap = DynamicPartitionMapImpl.create(ring);
        // We do not tolerate failures in this test. It is important since the
        // non-critical operations are done asynchronously and might not finishi
        // before checking the results.
        pkvs = PartitionedKeyValueService.create(new QuorumParameters(3, 3, 3), pmap);

        // Push the map to all the endpoints
        for (int i=0; i<NUM_EPTS-1; ++i) {
            skves[i].partitionMapService().updateMap(pmap);
        }
    }

    @Before
    public void setupHacks() {
        for (int i=0; i<NUM_EPTS; ++i) {
            Utils.setupRuleHacks(epts[i].kvs.rule);
            Utils.setupRuleHacks(epts[i].pms.rule);
        }
    }

    @Test
    public void testVersionTooOld() {
    	pmap.setVersion(1L);
    	skves[0].partitionMapService().updateMap(pmap);
    	pmap.setVersion(0L);
    	assertEquals(1L, skves[0].partitionMapService().getMapVersion());
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

        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());
        pkvs.getPartitionMap().addEndpoint(new byte[] {(byte)0xff, 0, 0, 0}, skves[NUM_EPTS-1], "", false);
        pkvs.getPartitionMap().syncAddEndpoint();
        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());
        for (int i=0; i<NUM_EPTS - 1; ++i) {
            skves[i].partitionMapService().updateMap(pkvs.getPartitionMap());
        }

        Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        // Force pmap updateMap (why not)
        pkvs.getPartitionMap().setVersion(0L);
        try {
            pkvs.createTable(TEST_TABLE, 12345);
            fail();
        } catch (VersionTooOldException e) {
            pkvs.createTable(TEST_TABLE, 12345);
        }
        assertEquals(1L, pkvs.getPartitionMap().getVersion());
        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);

        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(result0, epts[i].kvs.delegate.get(TEST_TABLE, cells0));
        }

        pkvs.getPartitionMap().finalizeAddEndpoint(new byte[] {(byte)0xff, 0, 0, 0});
        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());
        for (int i=0; i<NUM_EPTS - 1; ++i) {
            skves[i].partitionMapService().updateMap(pkvs.getPartitionMap());
        }

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        // This time without a forced updateMap
        assertEquals(2L, pkvs.getPartitionMap().getVersion());
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(2L, skves[i].partitionMapService().getMapVersion());
        }

        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        assertEquals(result1, epts[0].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[1].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(emptyResult, epts[2].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[3].kvs.delegate.get(TEST_TABLE, cells1));
    }

    @Test
    public void testRemoveEndpoint() {

        Map<Cell, Value> emptyResult = ImmutableMap.<Cell, Value>of();

        // First add the endpoint so that we can remove one
        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());

        pkvs.getPartitionMap().addEndpoint(new byte[] {(byte)0xff, 0, 0, 0}, skves[NUM_EPTS - 1], "", false);
        pkvs.getPartitionMap().syncAddEndpoint();
        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());
        for (int i=0; i<NUM_EPTS - 1; ++i) {
            skves[i].partitionMapService().updateMap(pkvs.getPartitionMap());
        }

        pkvs.getPartitionMap().finalizeAddEndpoint(new byte[] {(byte)0xff, 0, 0, 0});
        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());
        for (int i=0; i<NUM_EPTS - 1; ++i) {
            skves[i].partitionMapService().updateMap(pkvs.getPartitionMap());
        }

        pkvs.createTable(TEST_TABLE, 12345);

        pkvs.getPartitionMap().removeEndpoint(new byte[] {0, 0}, false);
        pkvs.getPartitionMap().syncRemoveEndpoint();
        for (int i=0; i<NUM_EPTS; ++i) {
            skves[i].partitionMapService().updateMap(pkvs.getPartitionMap());
        }

        Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(result0, epts[i].kvs.delegate.get(TEST_TABLE, cells0));
        }

        pkvs.getPartitionMap().finalizeRemoveEndpoint(new byte[] {0, 0});
        skves[0].partitionMapService().updateMap(pkvs.getPartitionMap());
        skves[2].partitionMapService().updateMap(pkvs.getPartitionMap());
        skves[3].partitionMapService().updateMap(pkvs.getPartitionMap());

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        assertEquals(result1, epts[0].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(emptyResult, epts[1].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[2].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[3].kvs.delegate.get(TEST_TABLE, cells1));
    }

    @Override
    protected KeyValueService getKeyValueService() {
        setUpPrivate();
        return Preconditions.checkNotNull(pkvs);
    }
}
