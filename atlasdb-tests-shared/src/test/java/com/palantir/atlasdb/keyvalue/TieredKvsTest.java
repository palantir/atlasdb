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
package com.palantir.atlasdb.keyvalue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TieredKeyValueService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class TieredKvsTest {

    private final TableReference tieredTable = TableReference.createWithEmptyNamespace("tiered_table");
    private final TableReference untieredTable = TableReference.createWithEmptyNamespace("untiered_table");
    private TieredKeyValueService tieredKvs;
    private KeyValueService primaryKvs;
    private KeyValueService secondaryKvs;

    @Before
    public void setup() {
        primaryKvs = new InMemoryKeyValueService(true);
        secondaryKvs = new InMemoryKeyValueService(true);
        tieredKvs = TieredKeyValueService.create(ImmutableSet.of(tieredTable), primaryKvs, secondaryKvs);

        Multimap<Cell, Value> values = ArrayListMultimap.create();
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                values.put(getCell(row, col), getValue(4*row + col, 4*row + col));
            }
        }
        secondaryKvs.putWithTimestamps(tieredTable, values);

        values = ArrayListMultimap.create();
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                if ((row + col) % 2 == 0) {
                    values.put(getCell(row, col), getValue(4*row + col, 100 + 4*row + col));
                }
            }
        }
        secondaryKvs.putWithTimestamps(tieredTable, values);

        values = ArrayListMultimap.create();
        for (int row = 0; row < 2; row++) {
            for (int col = 0; col < 4; col++) {
                values.put(getCell(row, col), getValue(4*row + col, 200 + 4*row + col));
            }
        }
        primaryKvs.putWithTimestamps(tieredTable, values);

        values = ArrayListMultimap.create();
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 2; col++) {
                values.put(getCell(row, col), getValue(4*row + col, 300 + 4*row + col));
            }
        }
        primaryKvs.putWithTimestamps(tieredTable, values);

        values = ArrayListMultimap.create();
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                if ((row + col) % 2 == 0) {
                    values.put(getCell(row, col), getValue(4*row + col, 4*row + col));
                }
            }
        }
        primaryKvs.putWithTimestamps(untieredTable, values);
    }

    @After
    public void tearDown() {
        tieredKvs.teardown();
        primaryKvs.teardown();
        secondaryKvs.teardown();
    }

    @Test
    public void testGet() {
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                Map<Cell, Value> result0 = tieredKvs.get(tieredTable, ImmutableMap.of(getCell(row, col), 0L));
                Map<Cell, Value> result100 = tieredKvs.get(tieredTable, ImmutableMap.of(getCell(row, col), 100L));
                Map<Cell, Value> result200 = tieredKvs.get(tieredTable, ImmutableMap.of(getCell(row, col), 200L));
                Map<Cell, Value> result300 = tieredKvs.get(tieredTable, ImmutableMap.of(getCell(row, col), 300L));
                Map<Cell, Value> result400 = tieredKvs.get(tieredTable, ImmutableMap.of(getCell(row, col), 400L));

                Assert.assertEquals(ImmutableMap.of(), result0);

                assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 4*row + col)), result100);

                if ((row + col) % 2 == 0) {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 100 + 4*row + col)), result200);
                } else {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 4*row + col)), result200);
                }

                if (row < 2) {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 200 + 4*row + col)), result300);
                } else if ((row + col) % 2 == 0) {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 100 + 4*row + col)), result300);
                } else {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 4*row + col)), result300);
                }

                if (col < 2) {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 300 + 4*row + col)), result400);
                } else if (row < 2) {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 200 + 4*row + col)), result400);
                } else if ((row + col) % 2 == 0) {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 100 + 4*row + col)), result400);
                } else {
                    assertEquals(ImmutableMap.of(getCell(row, col), getValue(4*row + col, 4*row + col)), result400);
                }
            }
        }
    }

    @Test
    public void testGetRows() {
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                Map<Cell, Value> rows0 = tieredKvs.getRows(tieredTable, ImmutableList.of(getRow(row)), ColumnSelection.all(), 0L);
                Map<Cell, Value> rows100 = tieredKvs.getRows(tieredTable, ImmutableList.of(getRow(row)), ColumnSelection.all(), 100L);
                Map<Cell, Value> rows200 = tieredKvs.getRows(tieredTable, ImmutableList.of(getRow(row)), ColumnSelection.all(), 200L);
                Map<Cell, Value> rows300 = tieredKvs.getRows(tieredTable, ImmutableList.of(getRow(row)), ColumnSelection.all(), 300L);
                Map<Cell, Value> rows400 = tieredKvs.getRows(tieredTable, ImmutableList.of(getRow(row)), ColumnSelection.all(), 400L);

                assertEquals(ImmutableMap.of(), rows0);

                assertEquals(ImmutableMap.of(
                        getCell(row, 0), getValue(4*row + 0, 4*row + 0),
                        getCell(row, 1), getValue(4*row + 1, 4*row + 1),
                        getCell(row, 2), getValue(4*row + 2, 4*row + 2),
                        getCell(row, 3), getValue(4*row + 3, 4*row + 3)), rows100);

                if (row % 2 == 0) {
                    assertEquals(ImmutableMap.of(
                            getCell(row, 0), getValue(4*row + 0, 100 + 4*row + 0),
                            getCell(row, 1), getValue(4*row + 1, 4*row + 1),
                            getCell(row, 2), getValue(4*row + 2, 100 + 4*row + 2),
                            getCell(row, 3), getValue(4*row + 3, 4*row + 3)), rows200);
                    if (row < 2) {
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 200 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 200 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 200 + 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 200 + 4*row + 3)), rows300);
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 300 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 300 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 200 + 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 200 + 4*row + 3)), rows400);
                    } else {
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 100 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 100 + 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 4*row + 3)), rows300);
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 300 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 300 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 100 + 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 4*row + 3)), rows400);
                    }
                } else {
                    assertEquals(ImmutableMap.of(
                            getCell(row, 0), getValue(4*row + 0, 4*row + 0),
                            getCell(row, 1), getValue(4*row + 1, 100 + 4*row + 1),
                            getCell(row, 2), getValue(4*row + 2, 4*row + 2),
                            getCell(row, 3), getValue(4*row + 3, 100 + 4*row + 3)), rows200);
                    if (row < 2) {
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 200 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 200 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 200 + 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 200 + 4*row + 3)), rows300);
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 300 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 300 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 200 + 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 200 + 4*row + 3)), rows400);
                    } else {
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 100 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 100 + 4*row + 3)), rows300);
                        assertEquals(ImmutableMap.of(
                                getCell(row, 0), getValue(4*row + 0, 300 + 4*row + 0),
                                getCell(row, 1), getValue(4*row + 1, 300 + 4*row + 1),
                                getCell(row, 2), getValue(4*row + 2, 4*row + 2),
                                getCell(row, 3), getValue(4*row + 3, 100 + 4*row + 3)), rows400);
                    }
                }
            }
        }
    }

    @Test
    public void testGetRange() {
        List<RowResult<Value>> range0 = ImmutableList.copyOf(tieredKvs.getRange(tieredTable, RangeRequest.all(), 0L));
        List<RowResult<Value>> range100 = ImmutableList.copyOf(tieredKvs.getRange(tieredTable, RangeRequest.all(), 100L));
        List<RowResult<Value>> range200 = ImmutableList.copyOf(tieredKvs.getRange(tieredTable, RangeRequest.all().withBatchHint(3), 200L));
        List<RowResult<Value>> range300 = ImmutableList.copyOf(tieredKvs.getRange(tieredTable, RangeRequest.all().withBatchHint(2), 300L));
        List<RowResult<Value>> range400 = ImmutableList.copyOf(tieredKvs.getRange(tieredTable, RangeRequest.all().withBatchHint(1), 400L));

        List<RowResult<Value>> expected;
        expected = Lists.newArrayList();
        assertEquals(expected, range0);

        expected = Lists.newArrayList();
        for (int row = 0; row < 4; row++) {
            SortedMap<byte[], Value> cols = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            for (int col = 0; col < 4; col++) {
                cols.put(getCol(col), getValue(4*row + col, 4*row + col));
            }
            expected.add(RowResult.create(getRow(row), cols));
        }
        assertEquals(expected, range100);

        expected = Lists.newArrayList();
        for (int row = 0; row < 4; row++) {
            SortedMap<byte[], Value> cols = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            for (int col = 0; col < 4; col++) {
                if ((row + col) % 2 == 0) {
                    cols.put(getCol(col), getValue(4*row + col, 100 + 4*row + col));
                } else {
                    cols.put(getCol(col), getValue(4*row + col, 4*row + col));
                }
            }
            expected.add(RowResult.create(getRow(row), cols));
        }
        assertEquals(expected, range200);

        expected = getExpectedRange300();
        assertEquals(expected, range300);

        expected = Lists.newArrayList();
        for (int row = 0; row < 4; row++) {
            SortedMap<byte[], Value> cols = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            for (int col = 0; col < 4; col++) {
                if (col < 2) {
                    cols.put(getCol(col), getValue(4*row + col, 300 + 4*row + col));
                } else if (row < 2) {
                    cols.put(getCol(col), getValue(4*row + col, 200 + 4*row + col));
                } else if ((row + col) % 2 == 0) {
                    cols.put(getCol(col), getValue(4*row + col, 100 + 4*row + col));
                } else {
                    cols.put(getCol(col), getValue(4*row + col, 4*row + col));
                }
            }
            expected.add(RowResult.create(getRow(row), cols));
        }
        assertEquals(expected, range400);
    }

    @Test
    public void testGetRangeWithCopy() {
        testGetRangeWithAction(new Runnable() {
            @Override
            public void run() {
                ClosableIterator<RowResult<Value>> iter = primaryKvs.getRange(tieredTable, RangeRequest.all(), Long.MAX_VALUE);
                while (iter.hasNext()) {
                    RowResult<Value> next = iter.next();
                    for (Entry<Cell, Value> entry : next.getCells()) {
                        secondaryKvs.putWithTimestamps(tieredTable, ImmutableMultimap.of(entry.getKey(), entry.getValue()));
                    }
                }
            }
        });
    }

    private void testGetRangeWithAction(Runnable action) {
        List<RowResult<Value>> expected = getExpectedRange300();
        for (int i = 0; i < expected.size(); i++) {
            ClosableIterator<RowResult<Value>> range300 = tieredKvs.getRange(tieredTable, RangeRequest.all().withBatchHint(2), 300L);
            for (int j = 0; j < expected.size(); j++) {
                if (i == j) {
                    action.run();
                }
                assertEquals(expected.get(j), range300.next());
            }
            assertFalse(range300.hasNext());
            tearDown();
            setup();
        }
    }

    @Test
    public void testGetRanges() {
        List<RowResult<Value>> expected = getExpectedRange300();
        List<Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>> results = Lists.newArrayListWithCapacity(20);
        List<RangeRequest> requests = Lists.newArrayList(
                RangeRequest.builder().prefixRange(getRow(0)).build(),
                RangeRequest.builder().prefixRange(getRow(1)).build(),
                RangeRequest.builder().prefixRange(getRow(2)).build(),
                RangeRequest.builder().prefixRange(getRow(3)).build());
        for (int i = 0; i < 20; i++) {
            results.add(tieredKvs.getFirstBatchForRanges(tieredTable, requests, 300));
        }
        for (int i = 0; i < 20; i++) {
            List<RowResult<Value>> actual = Lists.newArrayList();
            actual.addAll(results.get(i).get(requests.get(0)).getResults());
            actual.addAll(results.get(i).get(requests.get(1)).getResults());
            actual.addAll(results.get(i).get(requests.get(2)).getResults());
            actual.addAll(results.get(i).get(requests.get(3)).getResults());
            assertEquals(expected, actual);
        }
    }

    private List<RowResult<Value>> getExpectedRange300() {
        List<RowResult<Value>> expected = Lists.newArrayList();
        for (int row = 0; row < 4; row++) {
            SortedMap<byte[], Value> cols = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            for (int col = 0; col < 4; col++) {
                if (row < 2) {
                    cols.put(getCol(col), getValue(4*row + col, 200 + 4*row + col));
                } else if ((row + col) % 2 == 0) {
                    cols.put(getCol(col), getValue(4*row + col, 100 + 4*row + col));
                } else {
                    cols.put(getCol(col), getValue(4*row + col, 4*row + col));
                }
            }
            expected.add(RowResult.create(getRow(row), cols));
        }
        return expected;
    }

    @Test
    public void testSetup() {
        ClosableIterator<RowResult<Value>> range;
        Set<RowResult<Value>> results;
        Map<Cell, Value> cells;
        range = primaryKvs.getRange(tieredTable, RangeRequest.all(), 200);
        assertFalse(range.hasNext());

        range = primaryKvs.getRange(tieredTable, RangeRequest.all(), 300);
        results = Sets.newHashSet(range);
        assertEquals(2, results.size());
        cells = getCells(results);
        assertEquals(8, cells.size());
        for (int row = 0; row < 2; row++) {
            for (int col = 0; col < 4; col++) {
                assertEquals(getValue(4*row + col, 200 + 4*row + col), cells.get(getCell(row, col)));
            }
        }

        range = primaryKvs.getRange(tieredTable, RangeRequest.all(), 400);
        results = Sets.newHashSet(range);
        assertEquals(4, results.size());
        cells = getCells(results);
        assertEquals(12, cells.size());
        for (int row = 0; row < 2; row++) {
            for (int col = 0; col < 2; col++) {
                assertEquals(getValue(4*row + col, 300 + 4*row + col), cells.get(getCell(row, col)));
            }
        }
        for (int row = 0; row < 2; row++) {
            for (int col = 2; col < 4; col++) {
                assertEquals(getValue(4*row + col, 200 + 4*row + col), cells.get(getCell(row, col)));
            }
        }
        for (int row = 2; row < 4; row++) {
            for (int col = 0; col < 2; col++) {
                assertEquals(getValue(4*row + col, 300 + 4*row + col), cells.get(getCell(row, col)));
            }
        }

        range = secondaryKvs.getRange(tieredTable, RangeRequest.all(), 100);
        results = Sets.newHashSet(range);
        assertEquals(4, results.size());
        cells = getCells(results);
        assertEquals(16, cells.size());
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                assertEquals(getValue(4*row + col, 4*row + col), cells.get(getCell(row, col)));
            }
        }

        range = secondaryKvs.getRange(tieredTable, RangeRequest.all(), 200);
        results = Sets.newHashSet(range);
        assertEquals(4, results.size());
        cells = getCells(results);
        assertEquals(16, cells.size());
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                if ((row + col) % 2 == 0) {
                    assertEquals(getValue(4*row + col, 100 + 4*row + col), cells.get(getCell(row, col)));
                } else {
                    assertEquals(getValue(4*row + col, 4*row + col), cells.get(getCell(row, col)));
                }
            }
        }
    }

    private Map<Cell, Value> getCells(Set<RowResult<Value>> rowResults) {
        Map<Cell, Value> cells = Maps.newHashMap();
        for (RowResult<Value> result : rowResults) {
            for (Entry<Cell, Value> entry : result.getCells()) {
                cells.put(entry.getKey(), entry.getValue());
            }
        }
        return cells;
    }

    private Cell getCell(int row, int col) {
        return Cell.create(("row" + row).getBytes(), ("col" + col).getBytes());
    }

    private Value getValue(int val, long timestamp) {
        return Value.create(("val" + val).getBytes(), timestamp);
    }

    private byte[] getRow(int row) {
        return ("row" + row).getBytes();
    }

    private byte[] getCol(int col) {
        return ("col" + col).getBytes();
    }
}
