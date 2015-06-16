// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema.stream.generated;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTestMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables = StreamTestTableFactory.of();

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestStreamMetadataTable metaTable = tables.getStreamTestStreamMetadataTable(t);
        Collection<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = Lists.newArrayListWithCapacity(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of((Long) ValueType.VAR_LONG.convertToJava(cell.getRowName(), 0)));
        }
        Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestStreamStore.of(t, tables).deleteStreams(toDelete);
        return false;
    }
}