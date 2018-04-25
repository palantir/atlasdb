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
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
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

    public Optional<Long> nextSweepableTimestampPartition(int shard, boolean conservative) {
        long lastSweptPartitionFine = progress.getLastSweptTimestampPartition(shard, conservative);
        long lastSweptPartitionCoarse = SweepQueueUtils.partitionFineToCoarse(lastSweptPartitionFine);
        return nextSweepablePartition(shard, conservative, lastSweptPartitionCoarse, lastSweptPartitionFine);
    }

    private Optional<Long> nextSweepablePartition(int shard, boolean conservative, long sweptCoarse, long sweptFine) {
        long sweepTimestamp = timestampProvider.getSweepTimestamp(Sweeper.fromBoolean(conservative));
        long sweepTimestampFine = SweepQueueUtils.tsPartitionFine(sweepTimestamp);
        long sweepTimestampCoarse = SweepQueueUtils.tsPartitionCoarse(sweepTimestamp);
        long currentCoarse = sweptCoarse;

        while (currentCoarse <= sweepTimestampCoarse) {
            Optional<Long> candidateFine = getCandidatesInCoarsePartition(shard, currentCoarse, conservative).stream()
                    .filter(ts -> ts > sweptFine && ts < sweepTimestampFine)
                    .min(Long::compareTo);
            if (candidateFine.isPresent()) {
                return candidateFine;
            }
            currentCoarse++;
        }
        return Optional.empty();
    }

    List<Long> getCandidatesInCoarsePartition(long shard, long partitionCoarse, boolean conservative) {
        byte[] row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                shard, partitionCoarse, PersistableBoolean.of(conservative).persistToBytes())
                .persistToBytes();
        RangeRequest request = SweepQueueUtils.requestColsForRow(row);
        ClosableIterator<RowResult<Value>> rowResultIterator = kvs.getRange(TABLE_REF, request, SweepQueueUtils.CAS_TS);

        if (!rowResultIterator.hasNext()) {
            return ImmutableList.of();
        }

        RowResult<Value> rowResult = rowResultIterator.next();

        // todo(gmaretic): is it better to use EncodingUtils.decodeUnsignedVarLong? This seems slightly clearer
        return rowResult.getColumns().keySet().stream()
                .map(SweepableTimestampsTable.SweepableTimestampsColumn.BYTES_HYDRATOR::hydrateFromBytes)
                .map(SweepableTimestampsTable.SweepableTimestampsColumn.getTimestampModulusFun())
                .collect(Collectors.toList());
    }
}
