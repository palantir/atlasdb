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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InKvsPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.remoting.Utils;
import com.palantir.atlasdb.keyvalue.remoting.Utils.RemoteEndpoint;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.concurrent.PTExecutors;

import io.dropwizard.testing.junit.DropwizardClientRule;
import jersey.repackaged.com.google.common.collect.Iterators;

/**
 * This test is to make sure that out of date exceptions are handled in a proper way.
 *
 * @author htarasiuk
 *
 */
@Ignore
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

    private static final int NUM_EPTS = 4;

    // We do not tolerate failures in this test. It is important since the
    // non-critical operations are done asynchronously and might not finish
    // before checking the results.
    private static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 3, 3);

    static {
        assert NUM_EPTS >= QUORUM_PARAMETERS.getReplicationFactor();
        assert QUORUM_PARAMETERS.getReplicationFactor() == QUORUM_PARAMETERS.getReadFactor();
        assert QUORUM_PARAMETERS.getReplicationFactor() == QUORUM_PARAMETERS.getWriteFactor();
    }

    // This array is the actual remote side of the endpoints
    private final RemoteEndpoint[] epts = new RemoteEndpoint[NUM_EPTS]; {
        for (int i=0; i<NUM_EPTS; ++i) {
            KeyValueService kvs = new InMemoryKeyValueService(false);
            epts[i] = new RemoteEndpoint(kvs, InKvsPartitionMapService.createEmptyInMemory());
        }
    };

    // This array is the local "remoting" side of the endpoints
    private final SimpleKeyValueEndpoint[] skves = new SimpleKeyValueEndpoint[NUM_EPTS];
    private PartitionedKeyValueService pkvs;

    @Rule public DropwizardClientRule kvsRule1 = epts[0].kvs.rule;
    @Rule public DropwizardClientRule kvsRule2 = epts[1].kvs.rule;
    @Rule public DropwizardClientRule kvsRule3 = epts[2].kvs.rule;
    @Rule public DropwizardClientRule kvsRule4 = epts[3].kvs.rule;
    @Rule public DropwizardClientRule pmsRule1 = epts[0].pms.rule;
    @Rule public DropwizardClientRule pmsRule2 = epts[1].pms.rule;
    @Rule public DropwizardClientRule pmsRule3 = epts[2].pms.rule;
    @Rule public DropwizardClientRule pmsRule4 = epts[3].pms.rule;

    @After
    public void cleanupStuff() throws Exception {
        for (int i = 0; i < NUM_EPTS; ++i) {
            for (String tableName : epts[i].kvs.delegate.getAllTableNames()) {
                epts[i].kvs.delegate.dropTable(tableName);
            }
        }
        setUpPrivate();
        super.setUp();
    }

    private static final byte[] ept_key_0 = new byte[] {0};
    private static final byte[] ept_key_1 = new byte[] {0, 0};
    private static final byte[] ept_key_2 = new byte[] {0, 0, 0};
    private static final byte[] ept_key_3 = new byte[] {(byte) 0xff, 0, 0, 0};

    public void setUpPrivate() {
        for (int i=0; i<NUM_EPTS; ++i) {
            skves[i] = SimpleKeyValueEndpoint.create(epts[i].kvs.rule.baseUri().toString(), epts[i].pms.rule.baseUri().toString());
        }

        NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        ring.put(ept_key_0, skves[0]);
        ring.put(ept_key_1, skves[1]);
        ring.put(ept_key_2, skves[2]);
        // Do not insert skves[3] - it will be used later to test addEndpoint

        DynamicPartitionMap pmap = DynamicPartitionMapImpl.create(QUORUM_PARAMETERS, ring, PTExecutors.newCachedThreadPool());
        ImmutableList<PartitionMapService> mapServices = ImmutableList.<PartitionMapService> of(InMemoryPartitionMapService.create(pmap));
        pkvs = PartitionedKeyValueService.create(QUORUM_PARAMETERS, mapServices);

        // Push the map to all the endpoints
        pmap.pushMapToEndpoints();
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
        pkvs.getPartitionMap().setVersion(1L);

        skves[0].partitionMapService().updateMapIfNewer(pkvs.getPartitionMap());
        skves[1].partitionMapService().updateMapIfNewer(pkvs.getPartitionMap());
        skves[2].partitionMapService().updateMapIfNewer(pkvs.getPartitionMap());
        skves[3].partitionMapService().updateMapIfNewer(pkvs.getPartitionMap());
        pkvs.createTable("TABLE_NAME_2", AtlasDbConstants.GENERIC_TABLE_METADATA);

        pkvs.getPartitionMap().setVersion(0L);

        assertEquals(1L, skves[0].partitionMapService().getMapVersion());
        assertEquals(1L, skves[1].partitionMapService().getMapVersion());
        assertEquals(1L, skves[2].partitionMapService().getMapVersion());
        assertEquals(1L, skves[3].partitionMapService().getMapVersion());

    	try {
    	    // This must throw - client partition map is out of date
    		pkvs.put(TEST_TABLE, ImmutableMap.of(firstCell, "whatever".getBytes()), 0L);
    		fail();
    	} catch (ClientVersionTooOldException e) {
    		pkvs.put(TEST_TABLE, ImmutableMap.of(firstCell, "whatever".getBytes()), 0L);
            Assert.assertArrayEquals("whatever".getBytes(), pkvs.get(TEST_TABLE, ImmutableMap.of(firstCell, 1L)).get(firstCell).getContents());
    	}
    }

    @Test
    public void testMultiAddEndpoint() throws Exception {
        // This tests that the put function will block for long enough.
        for (int i=0; i<100; ++i) {
            testAddEndpoint();
            cleanupStuff();
        }
    }

    @Test
    public void testAddEndpoint() {
        final Map<Cell, Value> emptyResult = ImmutableMap.<Cell, Value>of();

        final Map<Cell, Long> cells0 = ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 1);
        final Map<Cell, byte[]> values0 = ImmutableMap.of(Cell.create(row0, column0), value00);
        final Map<Cell, Value> result0 = ImmutableMap.of(Cell.create(row0, column0), Value.create(value00, TEST_TIMESTAMP));

        pkvs.getPartitionMap().addEndpoint(ept_key_3, skves[NUM_EPTS-1]);
        pkvs.getPartitionMap().pushMapToEndpoints();

        pkvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        // Force pmap updateMap (why not)
        pkvs.getPartitionMap().setVersion(0L);

        try {
            // put is not retryable and must throw in this case
            pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);
            fail();
        } catch (ClientVersionTooOldException e) {
            // Expected
        }

        // Make sure that the new version was downloaded
        assertEquals(1L, pkvs.getPartitionMap().getVersion());

        // Retry the put
        pkvs.put(TEST_TABLE, values0, TEST_TIMESTAMP);

        // Make sure that the writes went to all 4 endpoints
        for (int i=0; i<NUM_EPTS; ++i) {
            Map<Cell, Value> testResult = epts[i].kvs.delegate.get(TEST_TABLE, cells0);
            assertEquals(result0, testResult);
        }

        // Finish the adding
        pkvs.getPartitionMap().backfillAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().promoteAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().pushMapToEndpoints();

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        // Make sure that all endpoints got the new map
        assertEquals(2L, pkvs.getPartitionMap().getVersion());
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(2L, skves[i].partitionMapService().getMapVersion());
        }

        // Finally make sure that after the add operation completes, writes go
        // to 3 endpoints only
        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        assertEquals(result1, epts[0].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[1].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(emptyResult, epts[2].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[3].kvs.delegate.get(TEST_TABLE, cells1));
    }

    @Test
    public void testRemoveEndpoint() {

        // First add the endpoint so that we can remove one
        pkvs.getPartitionMap().addEndpoint(ept_key_3, skves[NUM_EPTS - 1]);
        pkvs.getPartitionMap().backfillAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().promoteAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().pushMapToEndpoints();

        pkvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        // Begin the remove operation
        pkvs.getPartitionMap().removeEndpoint(ept_key_1);
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
        pkvs.getPartitionMap().backfillRemovedEndpoint(ept_key_1);
        pkvs.getPartitionMap().promoteRemovedEndpoint(ept_key_1);
        pkvs.getPartitionMap().pushMapToEndpoints();

        Map<Cell, Long> cells1 = ImmutableMap.of(Cell.create(row0, column1), TEST_TIMESTAMP + 1);
        Map<Cell, byte[]> values1 = ImmutableMap.of(Cell.create(row0, column1), value01);
        Map<Cell, Value> result1 = ImmutableMap.of(Cell.create(row0, column1), Value.create(value01, TEST_TIMESTAMP));

        // Now the data should not be sent to the removed endpoint anymore
        pkvs.put(TEST_TABLE, values1, TEST_TIMESTAMP);

        assertEquals(result1, epts[0].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[2].kvs.delegate.get(TEST_TABLE, cells1));
        assertEquals(result1, epts[3].kvs.delegate.get(TEST_TABLE, cells1));

        // And the data shall be removed from the removed endpoint
        assertFalse(epts[1].kvs.delegate.getAllTableNames().contains(TEST_TABLE));
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

    @Test
    public void testMultiRangeIteratorRetryTransparentlyOnVersionMismatch() throws Exception {
        // This tests that the put function will block for long enough.
        for (int i=0; i<50; ++i) {
            testRangeIteratorRetryTransparentlyOnVersionMismatch();
            cleanupStuff();
        }
    }

    @Test
    public void testRangeIteratorRetryTransparentlyOnVersionMismatch() {
        putTestDataForSingleTimestamp();
        RangeRequest allNonPaged = RangeRequest.builder().batchHint(1).build();

        for (int i=0; i<NUM_EPTS-1; ++i) {
            assertEquals(0L, epts[i].pms.service.getMapVersion());
        }

        ClosableIterator<RowResult<Value>> it = pkvs.getRange(TEST_TABLE, allNonPaged, Long.MAX_VALUE);

        // This first row goes through with the original map
        it.next();

        // Moving one endpoint is enough, our failure factor is 1.
        pkvs.getPartitionMap().addEndpoint(ept_key_3, skves[NUM_EPTS-1]);
        pkvs.getPartitionMap().backfillAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().promoteAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().removeEndpoint(ept_key_0);
        pkvs.getPartitionMap().backfillRemovedEndpoint(ept_key_0);
        pkvs.getPartitionMap().promoteRemovedEndpoint(ept_key_0);

        // Now the map gets updated on remote endpoints
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(4L, epts[i].pms.service.getMapVersion());
        }

        // This should trigger a retry
        assertEquals(2, Iterators.size(it));
    }

    @Test
    public void testMultiRangeWithHistoryIteratorRetryTransparentlyOnVersionMismatch() throws Exception {
        // This tests that the put function will block for long enough.
        for (int i=0; i<50; ++i) {
            testRangeWithHistoryIteratorRetryTransparentlyOnVersionMismatch();
            cleanupStuff();
        }
    }

    @Test
    public void testRangeWithHistoryIteratorRetryTransparentlyOnVersionMismatch() {
        putTestDataForSingleTimestamp();
        RangeRequest allNonPaged = RangeRequest.builder().batchHint(1).build();

        for (int i=0; i<NUM_EPTS-1; ++i) {
            assertEquals(0L, epts[i].pms.service.getMapVersion());
        }

        ClosableIterator<RowResult<Set<Value>>> it = pkvs.getRangeWithHistory(TEST_TABLE, allNonPaged, Long.MAX_VALUE);

        // This first row goes through with the original map
        it.next();

        // Moving one endpoint is enough, our failure factor is 1.
        pkvs.getPartitionMap().addEndpoint(ept_key_3, skves[NUM_EPTS-1]);
        pkvs.getPartitionMap().backfillAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().promoteAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().removeEndpoint(ept_key_0);
        pkvs.getPartitionMap().backfillRemovedEndpoint(ept_key_0);
        pkvs.getPartitionMap().promoteRemovedEndpoint(ept_key_0);

        // Now the map gets updated on remote endpoints
        assertEquals(4L, pkvs.getPartitionMap().getVersion());
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(4L, epts[i].pms.service.getMapVersion());
        }

        // This should trigger a retry
        assertEquals(2, Iterators.size(it));
    }

    @Test
    public void testMultiRangeOfTimestampsIteratorRetryTransparentlyOnVersionMismatch() throws Exception {
        // This tests that the put function will block for long enough.
        for (int i=0; i<50; ++i) {
            testRangeOfTimestampsIteratorRetryTransparentlyOnVersionMismatch();
            cleanupStuff();
        }
    }

    @Test
    public void testRangeOfTimestampsIteratorRetryTransparentlyOnVersionMismatch() {
        putTestDataForSingleTimestamp();
        RangeRequest allNonPaged = RangeRequest.builder().batchHint(1).build();

        for (int i=0; i<NUM_EPTS-1; ++i) {
            assertEquals(0L, epts[i].pms.service.getMapVersion());
        }

        ClosableIterator<RowResult<Set<Long>>> it = pkvs.getRangeOfTimestamps(TEST_TABLE, allNonPaged, Long.MAX_VALUE);

        // This first row goes through with the original map
        it.next();

        // Moving one endpoint is enough, our failure factor is 1.
        pkvs.getPartitionMap().addEndpoint(ept_key_3, skves[NUM_EPTS-1]);
        pkvs.getPartitionMap().backfillAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().promoteAddedEndpoint(ept_key_3);
        pkvs.getPartitionMap().removeEndpoint(ept_key_0);
        pkvs.getPartitionMap().backfillRemovedEndpoint(ept_key_0);
        pkvs.getPartitionMap().promoteRemovedEndpoint(ept_key_0);

        // Now the map gets updated on remote endpoints
        for (int i=0; i<NUM_EPTS; ++i) {
            assertEquals(4L, epts[i].pms.service.getMapVersion());
        }

        // This should trigger a retry
        assertEquals(2, Iterators.size(it));
    }

    @Override
    protected PartitionedKeyValueService getKeyValueService() {
        setUpPrivate();
        return Preconditions.checkNotNull(pkvs);
    }

    @Override
    protected boolean reverseRangesSupported() {
        return false;
    }
}
