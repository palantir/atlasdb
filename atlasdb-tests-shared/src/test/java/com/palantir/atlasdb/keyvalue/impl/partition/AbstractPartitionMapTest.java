package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;

public abstract class AbstractPartitionMapTest {

    static final String TABLE1 = "table1";
    static final int TABLE1_MAXSIZE = 128;
    static final int REPF = 3;
    static final int READF = 2;
    static final int WRITEF = 2;
    static final QuorumParameters qp = new QuorumParameters(REPF, READF, WRITEF);
    static protected final byte[][] points = new byte[][] {
        newByteArray(0x00, 0x00), newByteArray(0x00, 0x02), newByteArray(0x00, 0x05),
        newByteArray(0x01, 0x01), newByteArray(0x01, 0x03), newByteArray(0x01, 0x07),
        newByteArray(0x01, 0x0B) };

    static protected RangeRequest rangeFromTo(byte[] from, byte[] to) {
        boolean reverse = false;
        if (from.length > 0 && to.length > 0 && UnsignedBytes.lexicographicalComparator().compare(from, to) > 0) {
            reverse = true;
        }
        return RangeRequest.builder(reverse).startRowInclusive(from).endRowExclusive(to).build();
    }

    private PartitionMap tpm;
    protected abstract PartitionMap getPartitionMap(QuorumParameters qp,
                                                    NavigableMap<byte[], KeyValueEndpoint> ring);

    static protected byte[] newByteArray(int... bytes) {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; ++i) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }

    protected ArrayList<KeyValueService> services = Lists.<KeyValueService> newArrayList(
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false));

    private void testRangeIntervalsOk(final RangeRequest rangeRequest) {
        Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> result = tpm.getServicesForRangeRead(
                TABLE1,
                rangeRequest);
        assertTrue(rangeRequest.isEmptyRange() || result.size() > 0);
        if (rangeRequest.isEmptyRange()) {
            assertTrue(result.size() == 0);
            return;
        }
        Multiset<ConsistentRingRangeRequest> ranges = result.keys();
        Iterator<ConsistentRingRangeRequest> it = ranges.iterator();

        if (!it.hasNext()) {
            return;
        }

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
            assertTrue(lastRange.get().getEndExclusive().length == 0);
        } else {
            assertTrue(Arrays.equals(
                    lastRange.get().getEndExclusive(),
                    rangeRequest.getEndExclusive()));
        }

        // Check that the adjacent intervals make sense
        it = ranges.iterator();
        ConsistentRingRangeRequest crrr = it.next();
        int numRepeats = 1;
        while (it.hasNext()) {
            // Check that there is proper number of services for each subrange
            while (numRepeats++ < REPF) {
                ConsistentRingRangeRequest newCrrr = it.next();
                assertEquals(newCrrr, crrr);
            }
            if (it.hasNext() == false) {
                break;
            }

            // Check that the next subrange is adjacent to the previous one
            ConsistentRingRangeRequest newCrrr = it.next();
            numRepeats = 1;
            assertTrue(Arrays.equals(
                crrr.get().getEndExclusive(),
                newCrrr.get().getStartInclusive()));
            crrr = newCrrr;
        }
    }

    private void testRangeMappingsOk(RangeRequest rangeRequest) {
        Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> result = tpm.getServicesForRangeRead(
                TABLE1,
                rangeRequest);
        for (ConsistentRingRangeRequest subRange : result.keySet()) {
            Collection<KeyValueEndpoint> services = result.get(subRange);
            assertEquals(REPF, services.size());
            for (KeyValueEndpoint kvs : services) {
//                tpm.getServicesForRowsRead(
//                        TABLE1,
//                        ImmutableSet.of(subRange.get().getStartInclusive())).containsKey(kvs);
            }
        }
    }

    @Before
    public void setUp() {
        Preconditions.checkArgument(services.size() == points.length);
        NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (int i = 0; i < points.length; ++i) {
            ring.put(points[i], InMemoryKeyValueEndpoint.create(services.get(i), new PartitionMapServiceImpl()));
        }
        tpm = getPartitionMap(qp, ring);
//        tpm = BasicPartitionMap.create(qp, ring);
//        tpm = new DynamicPartitionMapImpl(qp, ring);
    }

    @Test
    public void testServicesForRead() {
//        byte[] row0 = newByteArray(0x00);
//        byte[] row1 = newByteArray(0x00, 0x00);
//        byte[] row2 = newByteArray(0x00, 0x01);
//        byte[] row3 = newByteArray(0x01, 0x0A);
//        byte[] row4 = newByteArray(0x01, 0x0B);
//        byte[] row5 = newByteArray(0x01, 0x0D);
//
//        Map<KeyValueService, NavigableSet<byte[]>> services0 = tpm.getServicesForRowsRead(TABLE1, ImmutableList.of(row0));
//        assertEquals(REPF, services0.size());
//        assertEquals(ImmutableSet.of(row0), services0.get(services.get(0)));
//        assertEquals(ImmutableSet.of(row0), services0.get(services.get(1)));
//        assertEquals(ImmutableSet.of(row0), services0.get(services.get(2)));
//
//        Map<KeyValueService, NavigableSet<byte[]>> services1 = tpm.getServicesForRowsRead(TABLE1, ImmutableList.of(row1));
//        assertEquals(REPF, services1.size());
//        assertEquals(ImmutableSet.of(row1), services1.get(services.get(1)));
//        assertEquals(ImmutableSet.of(row1), services1.get(services.get(2)));
//        assertEquals(ImmutableSet.of(row1), services1.get(services.get(3)));
//
//        Map<KeyValueService, NavigableSet<byte[]>> services2 = tpm.getServicesForRowsRead(TABLE1, ImmutableList.of(row2));
//        assertEquals(REPF, services2.size());
//        assertEquals(ImmutableSet.of(row2), services2.get(services.get(1)));
//        assertEquals(ImmutableSet.of(row2), services2.get(services.get(2)));
//        assertEquals(ImmutableSet.of(row2), services2.get(services.get(3)));
//
//        Map<KeyValueService, NavigableSet<byte[]>> services3 = tpm.getServicesForRowsRead(TABLE1, ImmutableList.of(row3));
//        assertEquals(REPF, services3.size());
//        assertEquals(ImmutableSet.of(row3), services3.get(services.get(6)));
//        assertEquals(ImmutableSet.of(row3), services3.get(services.get(0)));
//        assertEquals(ImmutableSet.of(row3), services3.get(services.get(1)));
//
//        Map<KeyValueService, NavigableSet<byte[]>> services4 = tpm.getServicesForRowsRead(TABLE1, ImmutableList.of(row4));
//        assertEquals(REPF, services4.size());
//        assertEquals(ImmutableSet.of(row4), services4.get(services.get(0)));
//        assertEquals(ImmutableSet.of(row4), services4.get(services.get(1)));
//        assertEquals(ImmutableSet.of(row4), services4.get(services.get(2)));
//
//        Map<KeyValueService, NavigableSet<byte[]>> services5 = tpm.getServicesForRowsRead(TABLE1, ImmutableList.of(row5));
//        assertEquals(REPF, services5.size());
//        assertEquals(ImmutableSet.of(row5), services5.get(services.get(0)));
//        assertEquals(ImmutableSet.of(row5), services5.get(services.get(1)));
//        assertEquals(ImmutableSet.of(row5), services5.get(services.get(2)));
//
//        Map<KeyValueService, NavigableSet<byte[]>> servicesEmpty = tpm.getServicesForRowsRead(TABLE1, ImmutableList.<byte[]>of());
//        assertEquals(0, servicesEmpty.size());
//    }
//
//    @Test
//    public void testServicesForRangeRead() {
//        byte[][][] sampleRangesArr = new byte[][][] {
//                new byte[][] { newByteArray(0), newByteArray(1) },
//                new byte[][] { newByteArray(0), newByteArray(0xff) },
//                new byte[][] { newByteArray(0), newByteArray(0xfe) },
//                new byte[][] { newByteArray(1), newByteArray(0xfe) },
//                new byte[][] { newByteArray(1), newByteArray(0xff) },
//                new byte[][] { newByteArray(0), newByteArray(0xfe, 0xaa) },
//                new byte[][] { newByteArray(0x01), newByteArray(0xfe, 0xaa) },
//                new byte[][] { newByteArray(0x01), newByteArray(0x01, 0x01) },
//                new byte[][] { newByteArray(0x01), newByteArray(0x01) },
//                new byte[][] { newByteArray(0xff), newByteArray(0x00) },
//                new byte[][] {
//                        newByteArray(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a),
//                        newByteArray(0x01, 0x00) },
//                new byte[][] { newByteArray(0x01, 0x02), newByteArray(0x01, 0x00) },
//                new byte[][] { newByteArray(0x01), newByteArray(0xff, 0xaa) },
//                new byte[][] { newByteArray(0x01, 0x02), newByteArray(0x02, 0x03) } };
//
//        final RangeRequest[] requests = new RangeRequest[sampleRangesArr.length];
//        for (int i = 0; i < requests.length; i++) {
//            int cmp = UnsignedBytes.lexicographicalComparator().compare(
//                    sampleRangesArr[i][0],
//                    sampleRangesArr[i][1]);
//            RangeRequest.Builder builder = RangeRequest.builder(cmp >= 0);
//            requests[i] = builder.startRowInclusive(sampleRangesArr[i][0]).endRowExclusive(
//                    sampleRangesArr[i][1]).build();
//        }
//
//        RangeRequest testRange = RangeRequest.reverseBuilder().startRowInclusive(
//                newByteArray(0xff, 0xff, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a)).endRowExclusive(
//                newByteArray(0x01, 0x00)).build();
//        if (testRange.isReverse() == false) {
//            testRangeIntervalsOk(testRange);
//            testRangeMappingsOk(testRange);
//        }
//
//        for (int i = 0; i < requests.length; i++) {
//            final RangeRequest rangeRequest = requests[i];
//            if (rangeRequest.isReverse() == false) {
//                testRangeIntervalsOk(rangeRequest);
//                testRangeMappingsOk(rangeRequest);
//            }
//        }
        // TODO:
    }

    @Test
    public void testIntervalDivision() {
        RangeRequest longRange = RangeRequest.builder().startRowInclusive(newByteArray(0, 1)).endRowExclusive(
                newByteArray(0, 7)).build();
        RangeRequest[] longRangeSubranges = new RangeRequest[] {
                rangeFromTo(newByteArray(0, 1), newByteArray(0, 2)),
                rangeFromTo(newByteArray(0, 2), newByteArray(0, 5)),
                rangeFromTo(newByteArray(0, 5), newByteArray(0, 7))
        };

        testRangeIntervalsOk(longRange);
        testRangeMappingsOk(longRange);

        Iterator<ConsistentRingRangeRequest> it = tpm.getServicesForRangeRead(TABLE1, longRange).keySet().iterator();
        for (RangeRequest rr : longRangeSubranges) {
            RangeRequest crr = it.next().get();
            assertEquals(rr, crr);
        }
    }

    @Test
    public void testEmptyRange() {
        RangeRequest emptyRange = rangeFromTo(newByteArray(1,1), newByteArray(1, 1));
        testRangeIntervalsOk(emptyRange);
        testRangeMappingsOk(emptyRange);
        assertTrue(emptyRange.isEmptyRange());
        assertTrue(tpm.getServicesForRangeRead(TABLE1, emptyRange).isEmpty());
    }

    @Test
    public void testFromZero() {
        RangeRequest fromZero = rangeFromTo(newByteArray(0, 0), newByteArray(0, 5));
        RangeRequest[] fromZeroDivision = new RangeRequest[] {
                rangeFromTo(newByteArray(0, 0), newByteArray(0, 2)),
                rangeFromTo(newByteArray(0, 2), newByteArray(0, 5))
        };
        testRangeIntervalsOk(fromZero);
        testRangeMappingsOk(fromZero);
        Iterator<ConsistentRingRangeRequest> it = tpm.getServicesForRangeRead(TABLE1, fromZero).keySet().iterator();
        for (RangeRequest rr : fromZeroDivision) {
            RangeRequest crr = it.next().get();
            assertEquals(rr, crr);
        }
    }

    @Test
    public void testNoLowerBound() {
        RangeRequest noLowerRange = rangeFromTo(newByteArray(), newByteArray(0, 4));
        RangeRequest[] noLowerDivision = new RangeRequest[] {
                rangeFromTo(RangeRequests.getFirstRowName(), newByteArray(0, 0)),
                rangeFromTo(newByteArray(0, 0), newByteArray(0, 2)),
                rangeFromTo(newByteArray(0, 2), newByteArray(0, 4))
        };
        testRangeIntervalsOk(noLowerRange);
        testRangeMappingsOk(noLowerRange);
        Iterator<ConsistentRingRangeRequest> it = tpm.getServicesForRangeRead(TABLE1, noLowerRange).keySet().iterator();
        for (RangeRequest rr : noLowerDivision) {
            RangeRequest crr = it.next().get();
            assertEquals(crr, rr);
        }
    }

    @Test
    public void testNoUpperBound() {
        RangeRequest noUpperBound = rangeFromTo(newByteArray(0, 6), newByteArray());
        RangeRequest[] noUpperDivision = new RangeRequest[] {
                rangeFromTo(newByteArray(0, 6), newByteArray(1, 1)),
                rangeFromTo(newByteArray(1, 1), newByteArray(1, 3)),
                rangeFromTo(newByteArray(1, 3), newByteArray(1, 7)),
                rangeFromTo(newByteArray(1, 7), newByteArray(1, 0x0B)),
                rangeFromTo(newByteArray(1, 0x0B), newByteArray())
        };
        testRangeIntervalsOk(noUpperBound);
        testRangeMappingsOk(noUpperBound);
        Iterator<ConsistentRingRangeRequest> it = tpm.getServicesForRangeRead(TABLE1, noUpperBound).keySet().iterator();
        for (RangeRequest rr : noUpperDivision) {
            RangeRequest crr = it.next().get();
            assertEquals(rr, crr);
        }
    }

    @Test
    public void testNoBound() {
        RangeRequest allRange = RangeRequest.all();
        RangeRequest[] allDivision = new RangeRequest[] {
                rangeFromTo(newByteArray(0), newByteArray(0, 0)),
                rangeFromTo(newByteArray(0, 0), newByteArray(0, 2)),
                rangeFromTo(newByteArray(0, 2), newByteArray(0, 5)),
                rangeFromTo(newByteArray(0, 5), newByteArray(1, 1)),
                rangeFromTo(newByteArray(1, 1), newByteArray(1, 3)),
                rangeFromTo(newByteArray(1, 3), newByteArray(1, 7)),
                rangeFromTo(newByteArray(1, 7), newByteArray(1, 0x0B)),
                rangeFromTo(newByteArray(1, 0x0B), newByteArray())
        };
        testRangeIntervalsOk(allRange);
        testRangeMappingsOk(allRange);
        Iterator<ConsistentRingRangeRequest> it = tpm.getServicesForRangeRead(TABLE1, allRange).keySet().iterator();
        for (RangeRequest rr : allDivision) {
            RangeRequest crr = it.next().get();
            assertEquals(rr, crr);
        }
    }

}
