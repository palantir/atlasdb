package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.common.concurrent.PTExecutors;

public class PartitionMapRackingTest {

    static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);

    static final String RACK0 = "rack0";
    static final String RACK1 = "rack1";
    static final String RACK2 = "rack2";

    static final String TABLE = "test_table";

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

    DynamicPartitionMapImpl dpmi;
    final KeyValueEndpoint[] epts = new KeyValueEndpoint[6];
    PartitionMapTestUtils testUtils;

    Map<KeyValueService, Set<byte[]>> svc024_row0;
    Map<KeyValueService, Set<byte[]>> svc124_row1;
    Map<KeyValueService, Set<byte[]>> svc234_row2;
    Map<KeyValueService, Set<byte[]>> svc342_row3;
    Map<KeyValueService, Set<byte[]>> svc402_row4;
    Map<KeyValueService, Set<byte[]>> svc502_row5;

    @Before
    public void setUp() {
        epts[0] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK0);
        epts[1] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK0);
        epts[2] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK1);
        epts[3] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK0);
        epts[4] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK2);
        epts[5] = InMemoryKeyValueEndpoint.create(new InMemoryKeyValueService(false), InMemoryPartitionMapService.createEmpty(), RACK2);

        svc024_row0 = ImmutableMap.of(
                epts[0].keyValueService(), ROW0_SINGLETON,
                epts[2].keyValueService(), ROW0_SINGLETON,
                epts[4].keyValueService(), ROW0_SINGLETON);

        svc124_row1 = ImmutableMap.of(
                epts[1].keyValueService(), ROW1_SINGLETON,
                epts[2].keyValueService(), ROW1_SINGLETON,
                epts[4].keyValueService(), ROW1_SINGLETON);

        svc234_row2 = ImmutableMap.of(
                epts[2].keyValueService(), ROW2_SINGLETON,
                epts[3].keyValueService(), ROW2_SINGLETON,
                epts[4].keyValueService(), ROW2_SINGLETON);

        svc342_row3 = ImmutableMap.of(
                epts[3].keyValueService(), ROW3_SINGLETON,
                epts[4].keyValueService(), ROW3_SINGLETON,
                epts[2].keyValueService(), ROW3_SINGLETON);

        svc402_row4 = ImmutableMap.of(
                epts[4].keyValueService(), ROW4_SINGLETON,
                epts[0].keyValueService(), ROW4_SINGLETON,
                epts[2].keyValueService(), ROW4_SINGLETON);

        svc502_row5 = ImmutableMap.of(
                epts[5].keyValueService(), ROW5_SINGLETON,
                epts[0].keyValueService(), ROW5_SINGLETON,
                epts[2].keyValueService(), ROW5_SINGLETON);

        NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        byte[] key = new byte[] {0};
        for (KeyValueEndpoint ept : epts) {
            ring.put(key, Preconditions.checkNotNull(ept));
            key = new byte[key.length + 1];
        }

        dpmi = DynamicPartitionMapImpl.create(QUORUM_PARAMETERS, ring, PTExecutors.newCachedThreadPool());

        testUtils = new PartitionMapTestUtils(dpmi, TABLE);
    }

    @Test
    public void testSimple() {
        testUtils.testRows(svc024_row0, ROW0_SINGLETON);
        testUtils.testRows(svc124_row1, ROW1_SINGLETON);
        testUtils.testRows(svc234_row2, ROW2_SINGLETON);
        testUtils.testRows(svc342_row3, ROW3_SINGLETON);
        testUtils.testRows(svc402_row4, ROW4_SINGLETON);
        testUtils.testRows(svc502_row5, ROW5_SINGLETON);
    }

}
