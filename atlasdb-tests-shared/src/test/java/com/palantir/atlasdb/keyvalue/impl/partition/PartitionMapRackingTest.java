package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.common.collect.Maps2;
import com.palantir.common.concurrent.PTExecutors;

public class PartitionMapRackingTest {

    static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);

    static final String RACK0 = "rack0";
    static final String RACK1 = "rack1";
    static final String RACK2 = "rack2";

    static final String TABLE = "test_table";

    static final byte[][] EPT_KEYS = new byte[][] {
        new byte[] {0},
        new byte[] {0,0},
        new byte[] {0,0,0},
        new byte[] {0,0,0,0},
        new byte[] {0,0,0,0,0},
        new byte[] {0,0,0,0,0,0}
    };

    static final byte[] ROW0 = new byte[] {1};
    static final byte[] ROW1 = new byte[] {0};
    static final byte[] ROW2 = new byte[] {0,0};
    static final byte[] ROW3 = new byte[] {0,0,0};
    static final byte[] ROW4 = new byte[] {0,0,0,0};
    static final byte[] ROW5 = new byte[] {0,0,0,0,0};
    static final byte[] ROW6 = new byte[] {0,0,0,0,0,0};

    static final Set<byte[]> ROW0_SINGLETON = ImmutableSet.of(ROW0);
    static final Set<byte[]> ROW1_SINGLETON = ImmutableSet.of(ROW1);
    static final Set<byte[]> ROW2_SINGLETON = ImmutableSet.of(ROW2);
    static final Set<byte[]> ROW3_SINGLETON = ImmutableSet.of(ROW3);
    static final Set<byte[]> ROW4_SINGLETON = ImmutableSet.of(ROW4);
    static final Set<byte[]> ROW5_SINGLETON = ImmutableSet.of(ROW5);

    static final Cell ROW0_COL0 = Cell.create(ROW0, new byte[] {0});
    static final Cell ROW1_COL0 = Cell.create(ROW1, new byte[] {0});
    static final Cell ROW2_COL0 = Cell.create(ROW2, new byte[] {0});
    static final Cell ROW3_COL0 = Cell.create(ROW3, new byte[] {0});
    static final Cell ROW4_COL0 = Cell.create(ROW4, new byte[] {0});
    static final Cell ROW5_COL0 = Cell.create(ROW5, new byte[] {0});

    DynamicPartitionMapImpl dpmi;
    final KeyValueEndpoint[] epts = new KeyValueEndpoint[6];
    PartitionMapTestUtils testUtils;

    Set<KeyValueService> svc0124;
    Set<KeyValueService> svc024;
    Set<KeyValueService> svc124;
    Set<KeyValueService> svc234;
    Set<KeyValueService> svc342;
    Set<KeyValueService> svc4012;
    Set<KeyValueService> svc402;
    Set<KeyValueService> svc412;
    Set<KeyValueService> svc5012;
    Set<KeyValueService> svc502;
    Set<KeyValueService> svc512;

    Map<KeyValueService, Set<byte[]>> svc024_row0;
    Map<KeyValueService, Set<byte[]>> svc124_row0;
    Map<KeyValueService, Set<byte[]>> svc124_row1;
    Map<KeyValueService, Set<byte[]>> svc234_row2;
    Map<KeyValueService, Set<byte[]>> svc342_row3;
    Map<KeyValueService, Set<byte[]>> svc402_row4;
    Map<KeyValueService, Set<byte[]>> svc412_row4;
    Map<KeyValueService, Set<byte[]>> svc502_row5;
    Map<KeyValueService, Set<byte[]>> svc512_row5;

    @Before
    public void setUp() {
        epts[0] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK0);
        epts[1] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK0);
        epts[2] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK1);
        epts[3] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK0);
        epts[4] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK2);
        epts[5] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK2);

        svc0124 = ImmutableSet.of(epts[0].keyValueService(), epts[1].keyValueService(), epts[2].keyValueService(), epts[4].keyValueService());
        svc024 = ImmutableSet.of(epts[0].keyValueService(), epts[2].keyValueService(), epts[4].keyValueService());
        svc124 = ImmutableSet.of(epts[1].keyValueService(), epts[2].keyValueService(), epts[4].keyValueService());
        svc234 = ImmutableSet.of(epts[2].keyValueService(), epts[3].keyValueService(), epts[4].keyValueService());
        svc342 = ImmutableSet.of(epts[3].keyValueService(), epts[4].keyValueService(), epts[2].keyValueService());
        svc4012 = ImmutableSet.of(epts[4].keyValueService(), epts[0].keyValueService(), epts[1].keyValueService(), epts[2].keyValueService());
        svc402 = ImmutableSet.of(epts[4].keyValueService(), epts[0].keyValueService(), epts[2].keyValueService());
        svc412 = ImmutableSet.of(epts[4].keyValueService(), epts[1].keyValueService(), epts[2].keyValueService());
        svc5012 = ImmutableSet.of(epts[5].keyValueService(), epts[0].keyValueService(), epts[1].keyValueService(), epts[2].keyValueService());
        svc502 = ImmutableSet.of(epts[5].keyValueService(), epts[0].keyValueService(), epts[2].keyValueService());
        svc512 = ImmutableSet.of(epts[5].keyValueService(), epts[1].keyValueService(), epts[2].keyValueService());

        svc024_row0 = Maps2.createConstantValueMap(svc024, ROW0_SINGLETON);
        svc124_row0 = Maps2.createConstantValueMap(svc124, ROW0_SINGLETON);
        svc124_row1 = Maps2.createConstantValueMap(svc124, ROW1_SINGLETON);
        svc234_row2 = Maps2.createConstantValueMap(svc234, ROW2_SINGLETON);
        svc342_row3 = Maps2.createConstantValueMap(svc342, ROW3_SINGLETON);
        svc402_row4 = Maps2.createConstantValueMap(svc402, ROW4_SINGLETON);
        svc412_row4 = Maps2.createConstantValueMap(svc412, ROW4_SINGLETON);
        svc502_row5 = Maps2.createConstantValueMap(svc502, ROW5_SINGLETON);
        svc512_row5 = Maps2.createConstantValueMap(svc512, ROW5_SINGLETON);

        NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        for (int i=0; i<epts.length; ++i) {
            ring.put(EPT_KEYS[i], Preconditions.checkNotNull(epts[i]));
        }

        dpmi = DynamicPartitionMapImpl.create(QUORUM_PARAMETERS, ring, PTExecutors.newCachedThreadPool());

        testUtils = new PartitionMapTestUtils(dpmi, TABLE);
    }

    @Test
    public void testRows() {
        testUtils.testRows(svc024_row0, ROW0_SINGLETON);
        testUtils.testRows(svc124_row1, ROW1_SINGLETON);
        testUtils.testRows(svc234_row2, ROW2_SINGLETON);
        testUtils.testRows(svc342_row3, ROW3_SINGLETON);
        testUtils.testRows(svc402_row4, ROW4_SINGLETON);
        testUtils.testRows(svc502_row5, ROW5_SINGLETON);
    }

    @Test
    public void testRowsWhenRemovingEndpoint() {
        // Endpoint with "leaving" status should still behave in same
        // way when used for read oprations (like getRows in this case).
        dpmi.removeEndpoint(EPT_KEYS[0]);
        testRows();
    }

    @Test
    public void testRowsWhenAddingEndpoint() {
        dpmi.removeEndpoint(EPT_KEYS[0]);
        dpmi.backfillRemovedEndpoint(EPT_KEYS[0]);
        dpmi.promoteRemovedEndpoint(EPT_KEYS[0]);

        dpmi.addEndpoint(EPT_KEYS[0], epts[0]);

        // Endpoint with "joining" status should be skipped when doing
        // read operations (like getRows in this case).
        testUtils.testRows(svc124_row0, ROW0_SINGLETON);
        testUtils.testRows(svc124_row1, ROW1_SINGLETON);
        testUtils.testRows(svc234_row2, ROW2_SINGLETON);
        testUtils.testRows(svc342_row3, ROW3_SINGLETON);
        testUtils.testRows(svc412_row4, ROW4_SINGLETON);
        testUtils.testRows(svc512_row5, ROW5_SINGLETON);

        dpmi.backfillAddedEndpoint(EPT_KEYS[0]);
        dpmi.promoteAddedEndpoint(EPT_KEYS[0]);

        // Should be back to normal now
        testRows();
    }

    @Test
    public void testWriteCells() {
        testUtils.testCellsWrite(svc024, ROW0_COL0);
        testUtils.testCellsWrite(svc124, ROW1_COL0);
        testUtils.testCellsWrite(svc234, ROW2_COL0);
        testUtils.testCellsWrite(svc342, ROW3_COL0);
        testUtils.testCellsWrite(svc402, ROW4_COL0);
        testUtils.testCellsWrite(svc502, ROW5_COL0);
    }

    @Test
    public void testWriteCellsWhenRemovingEndpoint() {
        // Endpoint with "leaving" status should be used, but also next
        // available endpoint should be used.
        dpmi.removeEndpoint(EPT_KEYS[0]);

        testUtils.testCellsWrite(svc0124, ROW0_COL0);
        testUtils.testCellsWrite(svc124, ROW1_COL0);
        testUtils.testCellsWrite(svc234, ROW2_COL0);
        testUtils.testCellsWrite(svc342, ROW3_COL0);
        testUtils.testCellsWrite(svc4012, ROW4_COL0);
        testUtils.testCellsWrite(svc5012, ROW5_COL0);

        dpmi.backfillRemovedEndpoint(EPT_KEYS[0]);
        dpmi.promoteRemovedEndpoint(EPT_KEYS[0]);
        dpmi.addEndpoint(EPT_KEYS[0], epts[0]);
        dpmi.backfillAddedEndpoint(EPT_KEYS[0]);
        dpmi.promoteAddedEndpoint(EPT_KEYS[0]);
        // Now everything should be back to normal
        testWriteCells();
    }

    @Test
    public void testWriteCellsWhenAddingEndpoint() {
        dpmi.removeEndpoint(EPT_KEYS[0]);
        dpmi.backfillRemovedEndpoint(EPT_KEYS[0]);
        dpmi.promoteRemovedEndpoint(EPT_KEYS[0]);
        dpmi.addEndpoint(EPT_KEYS[0], epts[0]);

        // Endpoint with "joining" status should be used, but also next
        // available endpoint should be used.
        testUtils.testCellsWrite(svc0124, ROW0_COL0);
        testUtils.testCellsWrite(svc124, ROW1_COL0);
        testUtils.testCellsWrite(svc234, ROW2_COL0);
        testUtils.testCellsWrite(svc342, ROW3_COL0);
        testUtils.testCellsWrite(svc4012, ROW4_COL0);
        testUtils.testCellsWrite(svc5012, ROW5_COL0);

        dpmi.backfillAddedEndpoint(EPT_KEYS[0]);
        dpmi.promoteAddedEndpoint(EPT_KEYS[0]);
        // Now everything should be back to normal
        testWriteCells();
    }

}
