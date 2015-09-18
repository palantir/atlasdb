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

import java.util.NavigableMap;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.common.concurrent.PTExecutors;

public class DynamicPartitionMapTest extends AbstractPartitionMapTest {

    private DynamicPartitionMapImpl dpm;

    final byte[] sampleRow = newByteArray(0, 3);
    final Cell sampleCell = Cell.create(sampleRow, newByteArray(0));
    final Set<Cell> sampleCellSet = ImmutableSet.of(sampleCell);

    final Set<KeyValueService> svc234 = ImmutableSet.of(services.get(2), services.get(3), services.get(4));
    final Set<KeyValueService> svc2345 = ImmutableSet.of(services.get(2), services.get(3), services.get(4), services.get(5));
    final Set<KeyValueService> svc23456 = ImmutableSet.of(services.get(2), services.get(3), services.get(4), services.get(5), services.get(6));
    final Set<KeyValueService> svc345 = ImmutableSet.of(services.get(3), services.get(4), services.get(5));

    /**
     *  (0) A  -  0 0
     *  (1) B  -  0 2
     *  (2) C  -  0 5
     *  (3) D  -  1 1
     *  (4) E  -  1 3
     *  (5) F  -  1 7
     *  (6) G  -  1 B
     *
     */

    @Override
    protected PartitionMap getPartitionMap(QuorumParameters qp,
                                           NavigableMap<byte[], KeyValueEndpoint> ring) {
        dpm = DynamicPartitionMapImpl.create(qp, ring, PTExecutors.newCachedThreadPool());
        return dpm;
    }

    @Test
    public void testRemoveEndpoint() {
        testCellsRead(svc234, sampleCell);
        testCellsWrite(svc234, sampleCell);

        assertEquals(true, dpm.removeEndpoint(newByteArray(0, 5)));
        /**
         * Now kvs (2) C is being removed.
         * The reads should still come from (2, 3, 4).
         * The writes should be directed to (2, 3, 4, 5).
         */
        testCellsRead(svc234, sampleCell);
        testCellsWrite(svc2345, sampleCell);

        dpm.backfillRemovedEndpoint(newByteArray(0, 5));
        dpm.promoteRemovedEndpoint(newByteArray(0, 5));
        /**
         * Now it should be back to normal, ie.
         * Reads -> (3, 4, 5)
         * Writes -> (3, 4, 5)
         */
        testCellsRead(svc345, sampleCell);
        testCellsWrite(svc345, sampleCell);

        assertEquals(true, dpm.addEndpoint(newByteArray(0, 5), endpoints.get(2), ""));
        dpm.backfillAddedEndpoint(newByteArray(0, 5));
        dpm.promoteAddedEndpoint(newByteArray(0, 5));
        testCellsRead(svc234, sampleCell);
        testCellsWrite(svc234, sampleCell);
    }

    @Test
    public void testAddEndpoint() {
        testCellsRead(svc234, sampleCell);
        testCellsWrite(svc234, sampleCell);

        assertEquals(true, dpm.removeEndpoint(newByteArray(0, 5)));
        dpm.backfillRemovedEndpoint(newByteArray(0, 5));
        dpm.promoteRemovedEndpoint(newByteArray(0, 5));
        testCellsRead(svc345, sampleCell);
        testCellsWrite(svc345, sampleCell);

        assertEquals(true, dpm.addEndpoint(newByteArray(0, 5), endpoints.get(2), ""));

        /**
         * Now kvs (2) C is being added.
         * The reads should be directed to (3, 4, 5).
         * Writes should be directed to (2, 3, 4, 5).
         */
        testCellsRead(svc345, sampleCell);
        testCellsWrite(svc2345, sampleCell);

        dpm.backfillAddedEndpoint(newByteArray(0, 5));
        dpm.promoteAddedEndpoint(newByteArray(0, 5));
        testCellsRead(svc234, sampleCell);
        testCellsWrite(svc234, sampleCell);
    }

    /**
     * This is to test add and removal running at the same time.
     */
    @Test
    public void testAddRemoveEndpoint() {
        assertEquals(true, dpm.removeEndpoint(newByteArray(0, 5)));
        assertEquals(false, dpm.removeEndpoint(newByteArray(1, 1)));
        dpm.backfillRemovedEndpoint(newByteArray(0, 5));
        dpm.promoteRemovedEndpoint(newByteArray(0, 5));
        dpm.pushMapToEndpoints();

        assertEquals(true, dpm.removeEndpoint(newByteArray(1, 1)));
        assertEquals(false, dpm.addEndpoint(newByteArray(0, 5), endpoints.get(2), ""));
        dpm.backfillRemovedEndpoint(newByteArray(1, 1));
        dpm.promoteRemovedEndpoint(newByteArray(1, 1));
        dpm.pushMapToEndpoints();
    }

}
