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
package com.palantir.atlasdb.schema.example;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.api.Transaction;

public final class DatasetsTable {

    private static final String TABLE_NAME = "datasets";

    private static final byte[] DATASET_COLUMN = {(byte) 'd'};

    private final ObjectMapper objectMapper;
    private final Transaction txn;

    public DatasetsTable(ObjectMapper objectMapper, Transaction txn) {
    	this.objectMapper = objectMapper;
        this.txn = txn;
    }

    public void putDataset(String branchId, String datasetId, Dataset dataset) {
        putDatasets(ImmutableMap.of(Key.of(branchId, datasetId), dataset));
    }

    public void putDatasets(Map<Key, Dataset> datasets) {
        ImmutableMap.Builder<Cell, byte[]> builder = ImmutableMap.builder();
        for (Entry<Key, Dataset> entry : datasets.entrySet()) {
            builder.put(Cell.create(entry.getKey().key(), DATASET_COLUMN), toByteArray(entry.getValue()));
        }
        txn.put(TABLE_NAME, builder.build());
    }

    public Dataset getDataset(String branchId, String datasetId) {
        Key key = Key.of(branchId, datasetId);
        return getDatasets(ImmutableSet.of(key)).get(key);
    }

    public Map<Key, Dataset> getDatasets(Set<Key> keys) {
        ImmutableSet.Builder<Cell> cells = ImmutableSet.builder();
        for (Key key : keys) {
            cells.add(Cell.create(key.key(), DATASET_COLUMN));
        }

        Map<Cell, byte[]> vals = txn.get(TABLE_NAME, cells.build());

        ImmutableMap.Builder<Key, Dataset> results = ImmutableMap.builder();
        for (Entry<Cell, byte[]> entry : vals.entrySet()) {
            results.put(Key.from(entry.getKey().getRowName()), toValue(entry.getValue(), Dataset.class));
        }

        return results.build();
    }

    public void putLatestTransaction(String branchId, String datasetId) {

    }

    public Transaction getLatestTransaction(String branchId, String datasetId) {
        return null;
    }

    public void putRow(String branchId, String datasetId, Row row) {
//    	txn.getRows(tableName, rows, columnSelection)
    }

    public Optional<Row> getRow(String branchId, String datasetId) {
        return null;
    }
//    
//    public Row getRows(Iterable<Key> keys) {
//        ImmutableSet.Builder<byte[]> rows = ImmutableSet.builder();
//        for(Key key : keys) {
//            rows.add(key.key());
//        }
//        
//        SortedMap<byte[], RowResult<byte[]>> results = txn.getRows(TABLE_NAME, rows.build(), ColumnSelection.all());
//        
//        List<Row> rowResults = Lists.newArrayList();
//        for (RowResult<byte[]> row : results.values()) {
//        	row.getCells();
//
//        }
//    }

    @Value.Immutable
    public abstract static class Key {
        // assume 40 chars
        public abstract String branchId();

        // assume 40 chars
        public abstract String datasetId();

        final byte[] key() {
            byte[] key = new byte[80];
            System.arraycopy(branchId().getBytes(Charsets.UTF_8), 0, key, 0, 40);
            System.arraycopy(datasetId().getBytes(Charsets.UTF_8), 0, key, 40, 40);
            return key;
        }

        public static final Key of(String branchId, String datasetId) {
            Preconditions.checkArgument(branchId.getBytes(Charsets.UTF_8).length <= 40, "Length <= 40");
            Preconditions.checkArgument(datasetId.getBytes(Charsets.UTF_8).length <= 40, "Length <= 40");
            return ImmutableKey.builder().branchId(branchId).datasetId(datasetId).build();
        }

        public static final Key from(byte[] arr) {
            Preconditions.checkArgument(arr.length == 80, "length must = 80");
            return ImmutableKey.builder()
                    .branchId(new String(arr, 0, 40))
                    .datasetId(new String(arr, 40, 40))
                    .build();
        }
    }

    public class Row {
		private RowResult<byte[]> row;
    	
    	public Row(RowResult<byte[]> row) {
    		this.row = row;
    	}
    	
        Dataset dataset() {
        	byte[] bytes = row.getColumns().get(DATASET_COLUMN);
        	return toValue(bytes, Dataset.class);
        }
        
        public boolean hasDataset() {
        	return row.getColumns().containsKey(DATASET_COLUMN);
        }
    }

    private <T> byte[] toByteArray(T val) {
        try {
            return objectMapper.writeValueAsBytes(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T toValue(byte[] arr, Class<T> type) {
        try {
            return objectMapper.readValue(arr, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}