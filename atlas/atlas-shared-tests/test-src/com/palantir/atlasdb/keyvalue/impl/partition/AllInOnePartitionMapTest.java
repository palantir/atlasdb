package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.AllInOnePartitionMap;

public class AllInOnePartitionMapTest {

//    TableAwarePartitionMapApi tpm;
    AllInOnePartitionMap tpm;
    static final String TABLE1 = "table1";
    static final int TABLE1_MAXSIZE = 128;
    static final int REPF = 3;
    static final int READF = 2;
    static final int WRITEF = 2;

    final static byte[][] points = new byte[][] {
            new byte[] { 0 },
            new byte[] { (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
//            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
            new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff , (byte) 0xff }};

    ArrayList<KeyValueService> services = Lists.<KeyValueService> newArrayList(
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false),
            new InMemoryKeyValueService(false));

    @Before
    public void setUp() {
        assert(services.size() == points.length);
        tpm = AllInOnePartitionMap.Create(REPF, READF, WRITEF, services, points);
//        tpm = AllInOnePartitionMap.Create();
        tpm.addTable(TABLE1, TABLE1_MAXSIZE);
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

        Set<KeyValueService> result = tpm.getServicesForRead(TABLE1, key);

        assertEquals(REPF, result.size());
        for (int i = 0; i < REPF; ++i) {
            int j = (higherPtIdx + i) % services.size();
            assertTrue(result.contains(services.get(j)));
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

    void testRangeIntervalsOk(RangeRequest rangeRequest) {
        if (rangeRequest.isReverse() == false) {
            return;
        }
        Preconditions.checkArgument(rangeRequest.getEndExclusive() != null && rangeRequest.getEndExclusive().length > 0);
        byte[] start = rangeRequest.getStartInclusive();
        byte[] end = rangeRequest.getEndExclusive();
        System.err.println("start=" + Arrays.toString(start));
        System.err.println("end=" + Arrays.toString(end));
        byte[] old = start;
        byte[] oldEnd = null;
        Multimap<RangeRequest, KeyValueService> result = tpm.getServicesForRangeRead(TABLE1, rangeRequest);
        assertTrue(rangeRequest.isEmptyRange() || result.size() > 0);
        // Make sure that the ranges are non-overlapping subranges of the original range and that their
        // union equals the original range.
        for (Map.Entry<RangeRequest, KeyValueService> e : result.entries()) {
            byte[] current = e.getKey().getStartInclusive();
            byte[] currentEnd = e.getKey().getEndExclusive();
            System.err.println("old=" + Arrays.toString(old));
            System.err.println("oldEnd=" + Arrays.toString(oldEnd));
            System.err.println("currrent=" + Arrays.toString(current));
            System.err.println("currentEnd=" + Arrays.toString(currentEnd));
            if (oldEnd == null) {
                // If this is the first interval
                // FIXME: This should fail on reverse range (!!)
                if (rangeRequest.isReverse()) {
                    assertTrue(Arrays.equals(currentEnd, end));
                } else {
                    assertTrue(Arrays.equals(current, start));
                }
            }
            assertTrue(UnsignedBytes.lexicographicalComparator().compare(currentEnd, end) <= 0);
            assertTrue(UnsignedBytes.lexicographicalComparator().compare(current, old) >= 0);
            int cmp = UnsignedBytes.lexicographicalComparator().compare(current, old);
            assertTrue(cmp >= 0);
            if (cmp == 0) {
                if (oldEnd != null) {
                    assertTrue(UnsignedBytes.lexicographicalComparator().compare(currentEnd, oldEnd) == 0);
                }
            } else if (cmp > 0) {
                assertTrue(UnsignedBytes.lexicographicalComparator().compare(oldEnd, current) == 0);
            }
            old = current;
            oldEnd = currentEnd;
        }
    }

    void testRangeMappingsOk(RangeRequest rangeRequest) {
        Multimap<RangeRequest, KeyValueService> result = tpm.getServicesForRangeRead(TABLE1, rangeRequest);
        for (RangeRequest subRange : result.keySet()) {
            Collection<KeyValueService> services = result.get(subRange);
            assertEquals(REPF, services.size());
            for (KeyValueService kvs : services) {
                assertTrue(tpm.getServicesForRead(TABLE1, subRange.getStartInclusive()).contains(kvs));
            }
        }
    }

    byte[] newByteArray(int... bytes) {
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

        for (RangeRequest rangeRequest : requests) {
            testRangeIntervalsOk(rangeRequest);
            testRangeMappingsOk(rangeRequest);
            System.err.println("OK");
        }
    }

}
