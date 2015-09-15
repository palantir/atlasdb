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
package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.NavigableMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InKvsPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.remoting.Utils;
import com.palantir.atlasdb.keyvalue.remoting.Utils.RemoteEndpoint;
import com.palantir.common.concurrent.PTExecutors;

import io.dropwizard.testing.junit.DropwizardClientRule;
import jersey.repackaged.com.google.common.collect.Iterators;

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
            KeyValueService kvs = new InMemoryKeyValueService(false);
            epts[i] = new RemoteEndpoint(kvs, InKvsPartitionMapService.createEmptyInMemory());
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

    @After
    public void cleanupStuff() {
        for (int i = 0; i < NUM_EPTS; ++i) {
            for (String tableName : epts[i].kvs.delegate.getAllTableNames()) {
                epts[i].kvs.delegate.dropTable(tableName);
            }
        }
        setUpPrivate();
    }

    public void setUpPrivate() {

        for (int i=0; i<NUM_EPTS; ++i) {
            skves[i] = new SimpleKeyValueEndpoint(epts[i].kvs.rule.baseUri().toString(), epts[i].pms.rule.baseUri().toString());
        }

        ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        ring.put(new byte[] {0},       skves[0]);
        ring.put(new byte[] {0, 0},    skves[1]);
        ring.put(new byte[] {0, 0, 0}, skves[2]);
        // Do not insert skves[3] - it will be used later to test addEndpoint

        pmap = DynamicPartitionMapImpl.create(new QuorumParameters(3, 3, 3), ring, PTExecutors.newCachedThreadPool());
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
        Cell firstCell = Cell.create(new byte[] {0}, new byte[] {0});
    	pmap.setVersion(1L);
    	skves[0].partitionMapService().updateMap(pmap);
    	pmap.setVersion(0L);
    	assertEquals(1L, skves[0].partitionMapService().getMapVersion());
    	try {
    		pkvs.createTable("TABLE_NAME_2", 12345);
    		pkvs.put(TEST_TABLE, ImmutableMap.of(firstCell, "whatever".getBytes()), 0L);
    		// This has to throw since table metadata is to be
    		// stored on all endpoints.
    		fail();
    	} catch (ClientVersionTooOldException e) {
    	    // The write could have succeeded for some endpoints, so first remove it
    	    // to avoid pkey violation exception.
    	    pkvs.delete(TEST_TABLE, ImmutableMultimap.of(firstCell, 0L));
    		pkvs.put(TEST_TABLE, ImmutableMap.of(firstCell, "whatever".getBytes()), 0L);
            Assert.assertArrayEquals("whatever".getBytes(), pkvs.get(TEST_TABLE, ImmutableMap.of(firstCell, 1L)).get(firstCell).getContents());
    	}
    }

    byte[] sampleKey = new byte[] {(byte)0xff, 0, 0, 0};

    @Test
    public void testMultiAddEndpoint() {
        // This tests that the put function will block for long enough.
        for (int i=0; i<100; ++i) {
            testAddEndpoint();
            cleanupStuff();
        }
    }

    @Test
    public void testAddEndpoint() {
        Cell sampleNonExistingCell = Cell.create("thisCell".getBytes(), "doesNotExist".getBytes());
        Map<Cell, Value> emptyResult = ImmutableMap.<Cell, Value>of();

        pkvs.getPartitionMap().addEndpoint(sampleKey, skves[NUM_EPTS-1], "");
        pkvs.getPartitionMap().pushMapToEndpoints();

        Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        pkvs.createTable(TEST_TABLE, 12345);

        // Force pmap updateMap (why not)
        pkvs.getPartitionMap().setVersion(0L);
        try {
            pkvs.delete(TEST_TABLE, ImmutableMultimap.of(sampleNonExistingCell, 0L));
            fail();
        } catch (ClientVersionTooOldException e) {
            pkvs.delete(TEST_TABLE, ImmutableMultimap.of(sampleNonExistingCell, 0L));
        }
        assertEquals(1L, pkvs.getPartitionMap().getVersion());
        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);

        for (int i=0; i<NUM_EPTS; ++i) {
            Map<Cell, Value> testResult = epts[i].kvs.delegate.get(TEST_TABLE, cells0);
            assertEquals(result0, testResult);
        }

        pkvs.getPartitionMap().promoteAddedEndpoint(sampleKey);
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

        // First add the endpoint so that we can remove one
        pkvs.getPartitionMap().addEndpoint(sampleKey, skves[NUM_EPTS - 1], "");
        pkvs.getPartitionMap().pushMapToEndpoints();

        pkvs.getPartitionMap().promoteAddedEndpoint(sampleKey);
        pkvs.getPartitionMap().pushMapToEndpoints();

        pkvs.createTable(TEST_TABLE, 12345);

        // Begin the remove operation
        byte[] anotherSampleKey = new byte[] {0, 0};
        pkvs.getPartitionMap().removeEndpoint(anotherSampleKey);
        pkvs.getPartitionMap().pushMapToEndpoints();

        Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        // Removal is in progress -> new data should be stored to all the nodes in the ring
        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(result0, epts[i].kvs.delegate.get(TEST_TABLE, cells0));
        }

        // Finish the remove operation
        pkvs.getPartitionMap().promoteRemovedEndpoint(anotherSampleKey);
        pkvs.getPartitionMap().pushMapToEndpoints();
        // Push the new map to the remove endpoint as well - in case someone will
        // still have the old map
        skves[NUM_EPTS - 1].partitionMapService().updateMap(pkvs.getPartitionMap());

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        // Now the data should not be sent to the removed endpoint anymore
        assertEquals(result1, epts[0].kvs.delegate.get(TEST_TABLE, cells1));
        Assert.assertFalse(epts[1].kvs.delegate.getAllTableNames().contains(TEST_TABLE));
        assertEquals(result1, epts[2].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[3].kvs.delegate.get(TEST_TABLE, cells1));
    }

    @Test
    public void testGetRangeThrowsOnNotEnoughReads() {
        putTestDataForSingleTimestamp();
        Iterators.size(pkvs.getRange(TEST_TABLE, RangeRequest.all(), Long.MAX_VALUE));
        epts[0].kvs.delegate.dropTable(TEST_TABLE);
        try {
            Iterators.size(pkvs.getRange(TEST_TABLE, RangeRequest.all(), Long.MAX_VALUE));
            Assert.fail("getRange must throw on not enough reads!");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testGetRangeWithHistoryThrowsOnNotEnoughReads() {
        putTestDataForSingleTimestamp();
        Iterators.size(pkvs.getRangeWithHistory(TEST_TABLE, RangeRequest.all(), Long.MAX_VALUE));
        epts[0].kvs.delegate.dropTable(TEST_TABLE);
        try {
            Iterators.size(pkvs.getRangeWithHistory(TEST_TABLE, RangeRequest.all(), Long.MAX_VALUE));
            Assert.fail("getRangeWithHistory must throw on not enough reads!");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testGetRangeOfTimestampsThrowsOnNotEnoughReads() {
        putTestDataForSingleTimestamp();
        Iterators.size(pkvs.getRangeOfTimestamps(TEST_TABLE, RangeRequest.all(), Long.MAX_VALUE));
        epts[0].kvs.delegate.dropTable(TEST_TABLE);
        try {
            Iterators.size(pkvs.getRangeOfTimestamps(TEST_TABLE, RangeRequest.all(), Long.MAX_VALUE));
            Assert.fail("getRangeOfTimestamps must throw on not enough reads!");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testGetRowsThrowsOnNotEnoughReads() {
        putTestDataForSingleTimestamp();
        pkvs.getRows(TEST_TABLE, ImmutableList.of(row0), ColumnSelection.all(), Long.MAX_VALUE);
        epts[0].kvs.delegate.dropTable(TEST_TABLE);
        try {
            pkvs.getRows(TEST_TABLE, ImmutableList.of(row0), ColumnSelection.all(), Long.MAX_VALUE);
            Assert.fail("getRows must throw on not enough reads!");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Override
    protected KeyValueService getKeyValueService() {
        setUpPrivate();
        return Preconditions.checkNotNull(pkvs);
    }
}
