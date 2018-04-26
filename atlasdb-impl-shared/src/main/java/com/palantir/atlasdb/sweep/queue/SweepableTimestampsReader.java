/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.PersistableBoolean;

public class SweepableTimestampsReader {
    private static final TableReference TABLE_REF = TargetedSweepTableFactory.of()
            .getSweepableTimestampsTable(null).getTableRef();
    private final KeyValueService kvs;
    private final SweepTimestampProvider timestampProvider;
    private final KvsSweepQueueProgress progress;

    SweepableTimestampsReader(KeyValueService kvs, SweepTimestampProvider provider) {
        this.kvs = kvs;
        this.timestampProvider = provider;
        this.progress = new KvsSweepQueueProgress(kvs);
    }

    public Optional<Long> nextSweepableTimestampPartition(ShardAndStrategy shardAndStrategy) {
        long lastSweptPartitionFine = progress.getLastSweptTimestampPartition(shardAndStrategy);
        long lastSweptPartitionCoarse = SweepQueueUtils.partitionFineToCoarse(lastSweptPartitionFine);
        return nextSweepablePartition(shardAndStrategy, lastSweptPartitionCoarse, lastSweptPartitionFine);
    }

    private Optional<Long> nextSweepablePartition(ShardAndStrategy shardAndStrategy, long sweptCoarse, long sweptFine) {
        long sweepTimestamp = timestampProvider.getSweepTimestamp(Sweeper.of(shardAndStrategy.strategy()).get());
        long sweepTimestampFine = SweepQueueUtils.tsPartitionFine(sweepTimestamp);
        long sweepTimestampCoarse = SweepQueueUtils.tsPartitionCoarse(sweepTimestamp);
        long currentCoarse = sweptCoarse;

        while (currentCoarse <= sweepTimestampCoarse) {
            Optional<Long> candidateFine = getCandidatesInCoarsePartition(shardAndStrategy, currentCoarse).stream()
                    .filter(ts -> ts > sweptFine && ts < sweepTimestampFine)
                    .min(Long::compareTo);
            if (candidateFine.isPresent()) {
                return candidateFine;
            }
            currentCoarse++;
        }
        return Optional.empty();
    }

    List<Long> getCandidatesInCoarsePartition(ShardAndStrategy shardAndStrategy, long partitionCoarse) {
        byte[] row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                shardAndStrategy.shard(),
                partitionCoarse,
                PersistableBoolean.of(shardAndStrategy.isConservative()).persistToBytes())
                .persistToBytes();
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(row)
                .endRowExclusive(RangeRequests.nextLexicographicName(row))
                .retainColumns(ColumnSelection.all())
                .batchHint(1)
                .build();
        ClosableIterator<RowResult<Value>> rowResultIterator = kvs.getRange(TABLE_REF, request, SweepQueueUtils.CAS_TS);

        if (!rowResultIterator.hasNext()) {
            return ImmutableList.of();
        }

        RowResult<Value> rowResult = rowResultIterator.next();

        return rowResult.getColumns().keySet().stream()
                .map(SweepableTimestampsTable.SweepableTimestampsColumn.BYTES_HYDRATOR::hydrateFromBytes)
                .map(SweepableTimestampsTable.SweepableTimestampsColumn.getTimestampModulusFun())
                .collect(Collectors.toList());
    }
}
