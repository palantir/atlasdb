package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.BasicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeComparator;

public class BasicPartitionMapTest {

    BasicPartitionMap tpm;
    static final String TABLE1 = "table1";
    static final int TABLE1_MAXSIZE = 128;
    static final int REPF = 3;
    static final int READF = 2;
    static final int WRITEF = 2;
    static final QuorumParameters qp = new QuorumParameters(REPF, READF, WRITEF);

    final static byte[][] points = new byte[][] {
            newByteArray(0x00, 0x00),
            newByteArray(0x00, 0x01),
            newByteArray(0x00, 0x02),
            newByteArray(0x01, 0x01),
            newByteArray(0x01, 0x02),
            newByteArray(0x01, 0x03),
            newByteArray(0x01, 0x04)
    };

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
        for (int i=0; i<points.length; ++i) {
            ring.put(points[i], services.get(i));
        }
        tpm = BasicPartitionMap.create(qp, ring);
        tpm.createTable(TABLE1, TABLE1_MAXSIZE);
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

        Map<KeyValueService, ? extends Iterable<byte[]>> result = tpm.getServicesForRowsRead(TABLE1, ImmutableSet.of(key));

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
        Preconditions.checkArgument(rangeRequest.getEndExclusive() != null && rangeRequest.getEndExclusive().length > 0);
        Preconditions.checkArgument(rangeRequest.getStartInclusive() != null && rangeRequest.getStartInclusive().length > 0);
        final boolean reverse = rangeRequest.isReverse();
        final byte[] globalStart = rangeRequest.getStartInclusive();
        final byte[] globalEnd = rangeRequest.getEndExclusive();

        Multimap<ConsistentRingRangeRequest, KeyValueService> result = tpm.getServicesForRangeRead(TABLE1, rangeRequest);
        assertTrue(rangeRequest.isEmptyRange() || result.size() > 0);

        System.err.println("rangeRequest=" + rangeRequest);
        System.err.println("globalStart=" + Arrays.toString(globalStart));
        System.err.println("gloablEnd=" + Arrays.toString(globalEnd));

        byte[] oldSubStart = null;
        byte[] oldSubEnd = null;

        System.err.println("Ranges are:");
        for (Map.Entry<ConsistentRingRangeRequest, ?> e : result.entries()) {
            System.err.println("[START=" + Arrays.toString(e.getKey().get().getStartInclusive()) + "; END="
                               + Arrays.toString(e.getKey().get().getEndExclusive()) + "]");
        }

        if (rangeRequest.isEmptyRange() == false) {
            // Check the first sub-interval
            if (!reverse) {
                byte[] firstSubStart = result.entries().iterator().next().getKey().get().getStartInclusive();
                assertTrue(Arrays.equals(firstSubStart, globalStart));
            } else {
                byte[] firstSubEnd = result.entries().iterator().next().getKey().get().getEndExclusive();
                System.err.println("firstSubEnd=" + Arrays.toString(firstSubEnd));
//                assertTrue(Arrays.equals(firstSubEnd, globalEnd));
            }

            // Check the last sub-interval
            if (!reverse) {
                Iterator<Entry<ConsistentRingRangeRequest, KeyValueService>> it = result.entries().iterator();
                byte[] lastSubEnd = null;
                while (it.hasNext()) {
                    lastSubEnd = it.next().getKey().get().getEndExclusive();
                }
                assertNotNull(lastSubEnd);
                System.err.println("lastSubEnd=" + Arrays.toString(lastSubEnd));
//                assertTrue(Arrays.equals(lastSubEnd, globalEnd));
            } else {
                Iterator<Entry<ConsistentRingRangeRequest, KeyValueService>> it = result.entries().iterator();
                byte[] lastSubStart = null;
                while (it.hasNext()) {
                    lastSubStart = it.next().getKey().get().getStartInclusive();
                }
                assertTrue(Arrays.equals(lastSubStart, globalStart));
            }
        }

        // Make sure that the ranges are non-overlapping subranges of the original range and that their
        // union equals the original range.
        for (Entry<ConsistentRingRangeRequest, KeyValueService> e : result.entries()) {
            byte[] subStart = e.getKey().get().getStartInclusive();
            byte[] subEnd = e.getKey().get().getEndExclusive();

            System.err.println("oldSubStart=" + Arrays.toString(oldSubStart));
            System.err.println("oldSubEnd=" + Arrays.toString(oldSubEnd));
            System.err.println("subStart=" + Arrays.toString(subStart));
            System.err.println("subEnd=" + Arrays.toString(subEnd));

            if (oldSubStart != null) {
                int cmpSubStart = ConsistentRingRangeComparator.compareBytes(
                        subStart,
                        oldSubStart,
                        reverse);
                int cmpSubEnd = ConsistentRingRangeComparator.compareBytes(
                        subEnd,
                        oldSubEnd,
                        reverse);
                System.err.println("cmpSubStart=" + cmpSubStart + " cmpSubEnd=" + cmpSubEnd
                        + " rev=" + reverse);
                assertTrue(Math.signum(cmpSubStart) == Math.signum(cmpSubEnd));
                // Make sure the intervals are in proper order
                assertTrue(cmpSubStart >= 0);
                if (cmpSubStart == 0) {
                    // Repeated interval
                    assertTrue(Arrays.equals(subEnd, oldSubEnd));
                } else {
                    // Next interval - make sure there is no gap in between
                    if (reverse) {
                        assertTrue(Arrays.equals(subEnd, oldSubStart));
                    } else {
                        assertTrue(Arrays.equals(subStart, oldSubEnd));
                    }
                }
            }
            oldSubStart = subStart;
            oldSubEnd = subEnd;
        }
    }

    void testRangeMappingsOk(RangeRequest rangeRequest) {
        Multimap<ConsistentRingRangeRequest, KeyValueService> result = tpm.getServicesForRangeRead(TABLE1, rangeRequest);
        for (ConsistentRingRangeRequest subRange : result.keySet()) {
            Collection<KeyValueService> services = result.get(subRange);
            assertEquals(REPF, services.size());
            for (KeyValueService kvs : services) {
                tpm.getServicesForRowsRead(TABLE1, ImmutableSet.of(subRange.get().getStartInclusive())).containsKey(kvs);
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
                new byte[][] {
                        newByteArray(0),
                        newByteArray(1)
                },
                new byte[][] {
                        newByteArray(0),
                        newByteArray(0xff)
                },
                new byte[][] {
                        newByteArray(0),
                        newByteArray(0xfe)
                },
                new byte[][] {
                        newByteArray(1),
                        newByteArray(0xfe)
                },
                new byte[][] {
                        newByteArray(1),
                        newByteArray(0xff)
                },
                new byte[][] {
                        newByteArray(0),
                        newByteArray(0xfe, 0xaa)
                },
                new byte[][] {
                        newByteArray(0x01),
                        newByteArray(0xfe, 0xaa)
                },
                new byte[][] {
                        newByteArray(0x01),
                        newByteArray(0x01, 0x01)
                },
                new byte[][] {
                        newByteArray(0x01),
                        newByteArray(0x01)
                },
                new byte[][] {
                        newByteArray(0xff),
                        newByteArray(0x00)
                },
                new byte[][] {
                        newByteArray(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a),
                        newByteArray(0x01, 0x00)
                },
                new byte[][] {
                        newByteArray(0x01, 0x02),
                        newByteArray(0x01, 0x00)
                },
                new byte[][] {
                        newByteArray(0x01),
                        newByteArray(0xff, 0xaa)
                },
                new byte[][] {
                        newByteArray(0x01, 0x02),
                        newByteArray(0x02, 0x03)
                }
        };

        final RangeRequest[] requests = new RangeRequest[sampleRangesArr.length];
        for (int i = 0; i < requests.length; i++) {
            int cmp = UnsignedBytes.lexicographicalComparator().compare(sampleRangesArr[i][0], sampleRangesArr[i][1]);
            RangeRequest.Builder builder = cmp >= 0 ? RangeRequest.reverseBuilder() : RangeRequest.builder();
            requests[i] = builder
                    .startRowInclusive(sampleRangesArr[i][0])
                    .endRowExclusive(sampleRangesArr[i][1])
                    .build();
        }

        //TODO: failing tests due to reverse
        RangeRequest testRange = RangeRequest.reverseBuilder().startRowInclusive(
                newByteArray(0xff, 0xff, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a)).endRowExclusive(
                newByteArray(0x01, 0x00)).build();
        testRangeIntervalsOk(testRange);
        testRangeMappingsOk(testRange);

        for (int i = 0; i < requests.length; i++) {
            final RangeRequest rangeRequest = requests[i];
            testRangeIntervalsOk(rangeRequest);
            testRangeMappingsOk(rangeRequest);
        }
    }

}
