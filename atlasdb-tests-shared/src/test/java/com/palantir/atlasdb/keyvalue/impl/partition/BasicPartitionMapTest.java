package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.BasicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;

public class BasicPartitionMapTest {

    BasicPartitionMap tpm;
    static final String TABLE1 = "table1";
    static final int TABLE1_MAXSIZE = 128;
    static final int REPF = 3;
    static final int READF = 2;
    static final int WRITEF = 2;
    static final QuorumParameters qp = new QuorumParameters(REPF, READF, WRITEF);

    final static byte[][] points = new byte[][] { newByteArray(0x00, 0x00),
            newByteArray(0x00, 0x01), newByteArray(0x00, 0x02), newByteArray(0x01, 0x01),
            newByteArray(0x01, 0x02), newByteArray(0x01, 0x03), newByteArray(0x01, 0x04) };

    ArrayList<KeyValueService> services = Lists.<KeyValueService> newArrayList(
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false));

    @Before
    public void setUp() {
        Preconditions.checkArgument(services.size() == points.length);
        NavigableMap<byte[], KeyValueService> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (int i = 0; i < points.length; ++i) {
            ring.put(points[i], services.get(i));
        }
        tpm = BasicPartitionMap.create(qp, ring);
        // TODO
        // tpm.createTable(TABLE1, TABLE1_MAXSIZE);
    }

    void testOneRead(byte[] key) {
        int higherPtIdx = 0;

        // If the key is bigger than last point, it is assigned to the first point (wrap-around)
        if (UnsignedBytes.lexicographicalComparator().compare(key, points[points.length - 1]) >= 0) {
            higherPtIdx = 0;
        } else {
            while (UnsignedBytes.lexicographicalComparator().compare(key, points[higherPtIdx]) >= 0) {
                higherPtIdx++;
            }
        }

        Map<KeyValueService, ? extends Iterable<byte[]>> result = tpm.getServicesForRowsRead(
                TABLE1,
                ImmutableSet.of(key));

        assertEquals(REPF, result.size());
        for (int i = 0; i < REPF; ++i) {
            int j = (higherPtIdx + i) % services.size();
            assertTrue(result.keySet().contains(services.get(j)));
        }
    }

    @Test
    public void testServicesForRead() {
        byte[] key;
        for (int i = 0; i < points.length; ++i) {
            key = points[i].clone();
            testOneRead(key);
            key[key.length - 1]--;
            testOneRead(key);
        }
    }

    void testRangeIntervalsOk(final RangeRequest rangeRequest) {
        Multimap<ConsistentRingRangeRequest, KeyValueService> result = tpm.getServicesForRangeRead(
                TABLE1,
                rangeRequest);
        assertTrue(rangeRequest.isEmptyRange() || result.size() > 0);
        if (rangeRequest.isEmptyRange()) {
            assertTrue(result.size() == 0);
            return;
        }
        Multiset<ConsistentRingRangeRequest> ranges = result.keys();
        Iterator<ConsistentRingRangeRequest> it = ranges.iterator();

        // Check that first interval makes sense
        ConsistentRingRangeRequest firstRange = it.next();
        if (rangeRequest.getStartInclusive().length == 0) {
            assertTrue(Arrays.equals(
                    RangeRequests.getFirstRowName(),
                    firstRange.get().getStartInclusive()));
        } else {
            assertTrue(Arrays.equals(
                    firstRange.get().getStartInclusive(),
                    rangeRequest.getStartInclusive()));
        }

        // Check that last interval makes sense
        ConsistentRingRangeRequest lastRange = firstRange;
        while (it.hasNext()) {
            lastRange = it.next();
        }
        if (rangeRequest.getEndExclusive().length == 0) {
            assertTrue(Arrays.equals(
                    RangeRequests.getLastRowName(),
                    lastRange.get().getEndExclusive()));
        } else {
            assertTrue(Arrays.equals(
                    lastRange.get().getEndExclusive(),
                    rangeRequest.getEndExclusive()));
        }

        // Check that the adjacent intervals make sense
        if (ranges.size() > 1) {
            it = ranges.iterator();
            ConsistentRingRangeRequest crrr = it.next();
            while (it.hasNext()) {
                ConsistentRingRangeRequest newCrrr = it.next();
                if (!newCrrr.equals(crrr)) {
                    assertTrue(Arrays.equals(
                            crrr.get().getEndExclusive(),
                            newCrrr.get().getStartInclusive()));
                }
                crrr = newCrrr;
            }
        }
    }

    void testRangeMappingsOk(RangeRequest rangeRequest) {
        Multimap<ConsistentRingRangeRequest, KeyValueService> result = tpm.getServicesForRangeRead(
                TABLE1,
                rangeRequest);
        for (ConsistentRingRangeRequest subRange : result.keySet()) {
            Collection<KeyValueService> services = result.get(subRange);
            assertEquals(REPF, services.size());
            for (KeyValueService kvs : services) {
                tpm.getServicesForRowsRead(
                        TABLE1,
                        ImmutableSet.of(subRange.get().getStartInclusive())).containsKey(kvs);
            }
        }
    }

    static byte[] newByteArray(int... bytes) {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; ++i) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }

    @Test
    public void testServicesForRangeRead() {
        byte[][][] sampleRangesArr = new byte[][][] {
                new byte[][] { newByteArray(0), newByteArray(1) },
                new byte[][] { newByteArray(0), newByteArray(0xff) },
                new byte[][] { newByteArray(0), newByteArray(0xfe) },
                new byte[][] { newByteArray(1), newByteArray(0xfe) },
                new byte[][] { newByteArray(1), newByteArray(0xff) },
                new byte[][] { newByteArray(0), newByteArray(0xfe, 0xaa) },
                new byte[][] { newByteArray(0x01), newByteArray(0xfe, 0xaa) },
                new byte[][] { newByteArray(0x01), newByteArray(0x01, 0x01) },
                new byte[][] { newByteArray(0x01), newByteArray(0x01) },
                new byte[][] { newByteArray(0xff), newByteArray(0x00) },
                new byte[][] {
                        newByteArray(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a),
                        newByteArray(0x01, 0x00) },
                new byte[][] { newByteArray(0x01, 0x02), newByteArray(0x01, 0x00) },
                new byte[][] { newByteArray(0x01), newByteArray(0xff, 0xaa) },
                new byte[][] { newByteArray(0x01, 0x02), newByteArray(0x02, 0x03) } };

        final RangeRequest[] requests = new RangeRequest[sampleRangesArr.length];
        for (int i = 0; i < requests.length; i++) {
            int cmp = UnsignedBytes.lexicographicalComparator().compare(
                    sampleRangesArr[i][0],
                    sampleRangesArr[i][1]);
            RangeRequest.Builder builder = cmp >= 0 ? RangeRequest.reverseBuilder() : RangeRequest.builder();
            requests[i] = builder.startRowInclusive(sampleRangesArr[i][0]).endRowExclusive(
                    sampleRangesArr[i][1]).build();
        }

        RangeRequest testRange = RangeRequest.reverseBuilder().startRowInclusive(
                newByteArray(0xff, 0xff, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a)).endRowExclusive(
                newByteArray(0x01, 0x00)).build();
        if (testRange.isReverse() == false) {
            testRangeIntervalsOk(testRange);
            testRangeMappingsOk(testRange);
        }

        for (int i = 0; i < requests.length; i++) {
            final RangeRequest rangeRequest = requests[i];
            if (rangeRequest.isReverse() == false) {
                testRangeIntervalsOk(rangeRequest);
                testRangeMappingsOk(rangeRequest);
            }
        }
    }

}
