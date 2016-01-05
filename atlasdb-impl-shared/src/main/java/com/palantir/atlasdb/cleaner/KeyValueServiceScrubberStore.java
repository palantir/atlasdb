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
package com.palantir.atlasdb.cleaner;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.ClosableIterator;

/**
 *
 * A ScrubberStore implemented as a table in the KeyValueService.
 *
 *
 * @author ejin
 */
public class KeyValueServiceScrubberStore implements ScrubberStore {

    private final KeyValueService keyValueService;

    public static ScrubberStore create(KeyValueService keyValueService) {
        keyValueService.createTable(AtlasDbConstants.SCRUB_TABLE, new TableMetadata(
                NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("cell", ValueType.BLOB))),
                new ColumnMetadataDescription(new DynamicColumnDescription(
                        NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("name", ValueType.STRING))),
                        ColumnValueDescription.forType(ValueType.STRING))),
                        ConflictHandler.IGNORE_ALL).persistToBytes());
        return new KeyValueServiceScrubberStore(keyValueService);
    }

    public static ScrubberStore createWithInMemoryKvs() {
        KeyValueService inMemoryKvs = new InMemoryKeyValueService(false);
        return create(inMemoryKvs);
    }

    private KeyValueServiceScrubberStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public void queueCellsForScrubbing(Multimap<String, Cell> tableNameToCells, long scrubTimestamp, int batchSize) {
        Multimap<Cell, String> cellToTableNames = HashMultimap.create();
        cellToTableNames = Multimaps.invertFrom(tableNameToCells, cellToTableNames);

        Map<Cell, byte[]> values = Maps.newHashMap();
        for (Map.Entry<Cell, Collection<String>> entry : cellToTableNames.asMap().entrySet()) {
            Cell cell = entry.getKey();
            Collection<String> tableNames = entry.getValue();
            // Doing the join here is safe--queueCellsForScrubbing is only called once per transaction
            // so we'll have all the table names for a given scrubTimestamp
            String joined = StringUtils.join(tableNames, AtlasDbConstants.SCRUB_TABLE_SEPARATOR_CHAR);
            values.put(cell, PtBytes.toBytes(joined));
        }
        for (List<Entry<Cell, byte[]>> batch : Iterables.partition(values.entrySet(), batchSize)) {
            Map<Cell, byte[]> batchMap = Maps.newHashMap();
            for (Entry<Cell, byte[]> e : batch) {
                batchMap.put(e.getKey(), e.getValue());
            }
            keyValueService.put(
                    AtlasDbConstants.SCRUB_TABLE,
                    batchMap,
                    scrubTimestamp);
        }
    }

    @Override
    public void markCellsAsScrubbed(Multimap<Cell, Long> cellToScrubTimestamp, int batchSize) {
        for (List<Entry<Cell, Long>> batch : Iterables.partition(cellToScrubTimestamp.entries(), batchSize)) {
            Multimap<Cell, Long> batchMultimap = HashMultimap.create();
            for (Entry<Cell, Long> e : batch) {
                batchMultimap.put(e.getKey(), e.getValue());
            }
            keyValueService.delete(
                    AtlasDbConstants.SCRUB_TABLE,
                    batchMultimap);
        }
    }

    @Override
    public SortedMap<Long, Multimap<String, Cell>> getCellsToScrub(final int maxCellsToScrub, long maxScrubTimestamp /* exclusive */) {
        if (maxCellsToScrub <= 0) {
            throw new IllegalArgumentException("Invalid maxCellsToScrub: " + maxCellsToScrub);
        }
        final SortedMap<Long, Multimap<String, Cell>> scrubTimestampToTableNameToCell = Maps.newTreeMap();
        ClosableIterator<RowResult<Value>> iterator = getIteratorToScrub(maxCellsToScrub, maxScrubTimestamp);
        try {
            Iterator<RowResult<Value>> limited = Iterators.limit(iterator, maxCellsToScrub);
            while (limited.hasNext()) {
                RowResult<Value> rowResult = limited.next();
                for (Map.Entry<Cell, Value> entry : rowResult.getCells()) {
                    Cell cell = entry.getKey();
                    Value value = entry.getValue();
                    long scrubTimestamp = value.getTimestamp();
                    String[] tableNames = StringUtils.split(
                            PtBytes.toString(value.getContents()),
                            AtlasDbConstants.SCRUB_TABLE_SEPARATOR_CHAR);
                    if (!scrubTimestampToTableNameToCell.containsKey(scrubTimestamp)) {
                        scrubTimestampToTableNameToCell.put(scrubTimestamp, HashMultimap.<String, Cell>create());
                    }
                    for (String tableName : tableNames) {
                        scrubTimestampToTableNameToCell.get(scrubTimestamp).put(tableName, cell);
                    }
                }
            }
            return scrubTimestampToTableNameToCell;
        } finally {
            iterator.close();
        }
    }

    private ClosableIterator<RowResult<Value>> getIteratorToScrub(final int maxCellsToScrub,
                                                                  long maxScrubTimestamp) {
        ClosableIterator<RowResult<Value>> iterator = keyValueService.getRange(
                AtlasDbConstants.SCRUB_TABLE,
                RangeRequest.builder().batchHint(maxCellsToScrub).build(),
                maxScrubTimestamp);
        return iterator;
    }

    @Override
    public int getNumberRemainingScrubCells(int maxCellsToScan) {
        ClosableIterator<RowResult<Value>> iterator = getIteratorToScrub(maxCellsToScan, Long.MAX_VALUE);
        try {
            return Iterators.size(Iterators.limit(iterator, maxCellsToScan));
        } finally {
            iterator.close();
        }
    }

}
