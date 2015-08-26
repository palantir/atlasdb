package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.NavigableMap;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;

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
        if (dpm == null) {
            dpm = new DynamicPartitionMapImpl(qp, ring);
        }
        return dpm;
    }

    private void printSet(Set<KeyValueService> set) {
        for (KeyValueService s : set) {
            System.err.println(services.lastIndexOf(s));
        }
    }

    @Test
    public void testRemoveEndpoint() {
//        assertEquals(svc234, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc234, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.removeEndpoint(newByteArray(0, 5), services.get(2), "");
//        /**
//         * Now kvs (2) C is being removed.
//         * The reads should still come from (2, 3, 4).
//         * The writes should be directed to (2, 3, 4, 5).
//         */
//        assertEquals(svc234, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc2345, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.finalizeRemoveEndpoint(newByteArray(0, 5), services.get(2));
//        /**
//         * Now it should be back to normal, ie.
//         * Reads -> (3, 4, 5)
//         * Writes -> (3, 4, 5)
//         */
//        assertEquals(svc345, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc345, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.addEndpoint(newByteArray(0, 5), services.get(2), "");
//        dpm.finalizeAddEndpoint(newByteArray(0, 5), services.get(2));
//        assertEquals(svc234, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc234, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
        // TODO:
    }

    @Test
    public void testAddEndpoint() {
//        assertEquals(svc234, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc234, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.removeEndpoint(newByteArray(0, 5), services.get(2), "");
//        dpm.finalizeRemoveEndpoint(newByteArray(0, 5), services.get(2));
//        assertEquals(svc345, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc345, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.addEndpoint(newByteArray(0, 5), services.get(2), "");
//        /**
//         * Now kvs (2) C is being added.
//         * The reads should be directed to (3, 4, 5).
//         * Writes should be directed to (2, 3, 4, 5).
//         */
//        assertEquals(svc345, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc2345, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.finalizeAddEndpoint(newByteArray(0, 5), services.get(2));
//
//        assertEquals(svc234, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc234, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
        // TODO:
    }

    /**
     * This is to test add and removal running at the same time.
     */
    @Test
    public void testAddRemoveEndpoint() {
//        assertEquals(svc234, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc234, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.removeEndpoint(newByteArray(0, 5), services.get(2), "");
//        dpm.finalizeRemoveEndpoint(newByteArray(0, 5), services.get(2));
//        assertEquals(svc345, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc345, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        dpm.removeEndpoint(newByteArray(1, 1), services.get(3), "");
//        dpm.addEndpoint(newByteArray(0, 5), services.get(2), "");
//        /**
//         * Originally
//         * writes -> (3,4,5)
//         * reads  -> (3,4,5)
//         *
//         * Add (2) C = (0,5)
//         * writes -> (2,3,4,5)
//         * reads  -> (3,4,5)
//         *
//         * Remove (3) D = (1,1)
//         * writes -> (2,3,4,5,6)
//         * reads  -> (3,4,5)
//         *
//         * Added (2) C = (0,5)
//         * writes -> (?)
//         * reads  -> (?)
//         *
//         * Now kvs (2) C = (0,5) is being added and kvs (3) D = (1,1) is being removed.
//         * Therefore writes should be directed to (2,3,4,5,6),
//         * and reads should be directed to (3,4,5).
//         */
//        assertEquals(svc345, dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet());
//        assertEquals(svc23456, dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet());
//
//        System.err.println("read0");
//        for (KeyValueService kvs : dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet()) {
//            System.err.print(services.lastIndexOf(kvs) + " ");
//        }
//        System.err.println();
//        System.err.println("write0");
//        for (KeyValueService  kvs : dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet()) {
//            System.err.print(services.lastIndexOf(kvs) + " ");
//        }
//        System.err.println();
//
//        dpm.finalizeAddEndpoint(newByteArray(0, 5), services.get(2));
//        System.err.println("read1");
//        for (KeyValueService kvs : dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet()) {
//            System.err.print(services.lastIndexOf(kvs) + " ");
//        }
//        System.err.println();
//        System.err.println("write1");
//        for (KeyValueService  kvs : dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet()) {
//            System.err.print(services.lastIndexOf(kvs) + " ");
//        }
//        System.err.println();
//
//        dpm.finalizeRemoveEndpoint(newByteArray(1, 1), services.get(3));
//        System.err.println("read2");
//        for (KeyValueService kvs : dpm.getServicesForCellsRead(TABLE1, sampleCellSet).keySet()) {
//            System.err.print(services.lastIndexOf(kvs) + " ");
//        }
//        System.err.println();
//        System.err.println("write2");
//        for (KeyValueService  kvs : dpm.getServicesForCellsWrite(TABLE1, sampleCellSet).keySet()) {
//            System.err.print(services.lastIndexOf(kvs) + " ");
//        }
//        System.err.println();
        // TODO:
    }

}
