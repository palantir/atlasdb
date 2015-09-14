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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.common.collect.Maps2;
import com.palantir.util.Pair;

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

    protected ArrayList<KeyValueEndpoint> endpoints = Lists.<KeyValueEndpoint> newArrayList(
            InMemoryKeyValueEndpoint.create(services.get(0), InMemoryPartitionMapService.createEmpty()),
            InMemoryKeyValueEndpoint.create(services.get(1), InMemoryPartitionMapService.createEmpty()),
            InMemoryKeyValueEndpoint.create(services.get(2), InMemoryPartitionMapService.createEmpty()),
            InMemoryKeyValueEndpoint.create(services.get(3), InMemoryPartitionMapService.createEmpty()),
            InMemoryKeyValueEndpoint.create(services.get(4), InMemoryPartitionMapService.createEmpty()),
            InMemoryKeyValueEndpoint.create(services.get(5), InMemoryPartitionMapService.createEmpty()),
            InMemoryKeyValueEndpoint.create(services.get(6), InMemoryPartitionMapService.createEmpty()));

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
                final Map<KeyValueService, Set<byte[]>> entries = Maps.newHashMap();
                tpm.runForRowsRead(TABLE1, ImmutableSet.of(subRange.get().getStartInclusive()), new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
                    @Override
                    public Void apply(Pair<KeyValueService, Iterable<byte[]>> input) {
                                entries.put(input.lhSide, ImmutableSortedSet
                                                .<byte[]> orderedBy(UnsignedBytes.lexicographicalComparator())
                                                .addAll(input.rhSide).build());
                        return null;
                    }
                });
                assertTrue(entries.containsKey(kvs.keyValueService()));
            }
        }
    }

    @Before
    public void setUp() {
        Preconditions.checkArgument(services.size() == points.length);
        NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (int i = 0; i < points.length; ++i) {
            ring.put(points[i], endpoints.get(i));
        }
        tpm = getPartitionMap(qp, ring);
//        tpm = BasicPartitionMap.create(qp, ring);
//        tpm = new DynamicPartitionMapImpl(qp, ring);
    }

    protected void testRows(Map<KeyValueService, Set<byte[]>> expected, Collection<byte[]> rows) {
        final Map<KeyValueService, Set<byte[]>> result = Maps.newHashMap();
        tpm.runForRowsRead(TABLE1, rows, new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
            @Override
            public Void apply(@Nullable Pair<KeyValueService, Iterable<byte[]>> input) {
                result.put(input.lhSide, ImmutableSortedSet
                                .<byte[]> orderedBy(UnsignedBytes.lexicographicalComparator())
                                .addAll(input.rhSide).build());
                return null;
            }
        });
        assertEquals(expected, result);
    }

    protected void testCellsRead(Map<KeyValueService, Set<Cell>> expected, Set<Cell> cells) {
        final Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        tpm.runForCellsRead(TABLE1, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
            @Override
            public Void apply(Pair<KeyValueService, Set<Cell>> input) {
                result.put(input.lhSide, input.rhSide);
                return null;
            }
        });
        assertEquals(expected, result);
    }

    protected void testCellsRead(Set<KeyValueService> expected, Cell cell) {
        Map<KeyValueService, Set<Cell>> expectedMap = Maps2
                .<KeyValueService, Set<Cell>> createConstantValueMap(expected, ImmutableSet.of(cell));
        testCellsRead(expectedMap, ImmutableSet.of(cell));
    }

    protected void testCellsWrite(Map<KeyValueService, Set<Cell>> expected, Set<Cell> cells) {
        final Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        tpm.runForCellsWrite(TABLE1, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
            @Override
            public Void apply(Pair<KeyValueService, Set<Cell>> input) {
                result.put(input.lhSide, input.rhSide);
                return null;
            }
        });
        assertEquals(expected, result);
    }

    protected void testCellsWrite(Set<KeyValueService> expected, Cell cell) {
        Map<KeyValueService, Set<Cell>> expectedMap = Maps2
                .<KeyValueService, Set<Cell>> createConstantValueMap(expected, ImmutableSet.of(cell));
        testCellsWrite(expectedMap, ImmutableSet.of(cell));
    }

    @Test
    public void testServicesForRead() {
        byte[] row0 = newByteArray(0x00);
        byte[] row1 = newByteArray(0x00, 0x00);
        byte[] row2 = newByteArray(0x00, 0x01);
        byte[] row3 = newByteArray(0x01, 0x0A);
        byte[] row4 = newByteArray(0x01, 0x0B);
        byte[] row5 = newByteArray(0x01, 0x0D);

        tpm.runForRowsRead(TABLE1, ImmutableList.of(row0), new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
			@Override
			public Void apply(@Nullable Pair<KeyValueService, Iterable<byte[]>> input) {
				return null;
			}
		});

        Map<KeyValueService, Set<byte[]>> expected0 = ImmutableMap.<KeyValueService, Set<byte[]>>of(services.get(0), ImmutableSet.of(row0),
        																							services.get(1), ImmutableSet.of(row0),
        																							services.get(2), ImmutableSet.of(row0));
        testRows(expected0, ImmutableList.of(row0));

        Map<KeyValueService, Set<byte[]>> expected1 = ImmutableMap.<KeyValueService, Set<byte[]>>of(services.get(1), ImmutableSet.of(row1),
                                                                                                    services.get(2), ImmutableSet.of(row1),
                                                                                                    services.get(3), ImmutableSet.of(row1));
        testRows(expected1, ImmutableList.of(row1));

        Map<KeyValueService, Set<byte[]>> expected2 = ImmutableMap.<KeyValueService, Set<byte[]>>of(services.get(1), ImmutableSet.of(row2),
                                                                                                    services.get(2), ImmutableSet.of(row2),
                                                                                                    services.get(3), ImmutableSet.of(row2));
        testRows(expected2, ImmutableList.of(row2));

        Map<KeyValueService, Set<byte[]>> expected3 = ImmutableMap.<KeyValueService, Set<byte[]>>of(services.get(6), ImmutableSet.of(row3),
                                                                                                    services.get(0), ImmutableSet.of(row3),
                                                                                                    services.get(1), ImmutableSet.of(row3));
        testRows(expected3, ImmutableList.of(row3));

        Map<KeyValueService, Set<byte[]>> expected4 = ImmutableMap.<KeyValueService, Set<byte[]>>of(services.get(0), ImmutableSet.of(row4),
                                                                                                    services.get(1), ImmutableSet.of(row4),
                                                                                                    services.get(2), ImmutableSet.of(row4));
        testRows(expected4, ImmutableList.of(row4));

        Map<KeyValueService, Set<byte[]>> expected5 = ImmutableMap.<KeyValueService, Set<byte[]>>of(services.get(0), ImmutableSet.of(row5),
                                                                                                    services.get(1), ImmutableSet.of(row5),
                                                                                                    services.get(2), ImmutableSet.of(row5));
        testRows(expected5, ImmutableList.of(row5));

        Map<KeyValueService, Set<byte[]>> expectedEmpty = ImmutableMap.<KeyValueService, Set<byte[]>>of();
        testRows(expectedEmpty, ImmutableList.<byte[]>of());
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
            RangeRequest.Builder builder = RangeRequest.builder(cmp >= 0);
            requests[i] = builder.startRowInclusive(sampleRangesArr[i][0]).endRowExclusive(
                    sampleRangesArr[i][1]).build();
        }

        RangeRequest testRange = RangeRequest.reverseBuilder().startRowInclusive(
                newByteArray(0xff, 0xff, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a)).endRowExclusive(
                newByteArray(0x01, 0x00)).build();
        // TODO:
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
