/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;

public class TokenRangeWritesLogger {
    private static final Logger log = LoggerFactory.getLogger(TokenRangeWritesLogger.class);

    @VisibleForTesting
    static final long THRESHOLD_WRITES_PER_TABLE = 100_000L;

    @VisibleForTesting
    volatile RangeMap<LightweightOppToken, ConcurrentMap<TableReference, Long>> tokenRangeWritesPerTable =
            ImmutableRangeMap.of();
    @VisibleForTesting
    ConcurrentMap<TableReference, Long> writesPerTable;

    public TokenRangeWritesLogger(Set<Range<LightweightOppToken>> ranges) {
        ImmutableRangeMap.Builder<LightweightOppToken, ConcurrentMap<TableReference, Long>> newMap =
                ImmutableRangeMap.builder();
        ranges.stream().forEach(range -> newMap.put(range, new ConcurrentHashMap<>()));
        tokenRangeWritesPerTable = newMap.build();
        writesPerTable = new ConcurrentHashMap<>();
    }

    public static TokenRangeWritesLogger createFromClientPool(CassandraClientPoolImpl clientPool) {
        return new TokenRangeWritesLogger(clientPool.getTokenRanges());

    }

    public <V> void  markWritesForTable(Set<Map.Entry<Cell, V>> entries, TableReference tableRef) {
        entries.forEach(entry -> {
            ConcurrentMap<TableReference, Long> tableMap = tokenRangeWritesPerTable
                    .get(new LightweightOppToken(entry.getKey().getRowName()));

            if (tableMap == null) {
                return;
            }

            tableMap.merge(tableRef, 1L, (fst, snd) -> fst + snd);
        });
        writesPerTable.merge(tableRef, Long.valueOf(entries.size()), (fst, snd) -> fst + snd);
        logIfOverThreshold(tableRef);
    }

    private void logIfOverThreshold(TableReference tableRef) {
        if (writesPerTable.getOrDefault(tableRef, 0L) > THRESHOLD_WRITES_PER_TABLE) {
            log(tableRef);
            writesPerTable.replace(tableRef, 0L);
        }
    }

    private void log(TableReference tableRef) {
        ImmutableMap.Builder<Range<LightweightOppToken>, Long> writesAsMapBuilder = ImmutableMap.builder();
        tokenRangeWritesPerTable.asMapOfRanges().entrySet().forEach(entry -> {
            long numWrites = entry.getValue().getOrDefault(tableRef, 0L);
            if (numWrites > 0) {
                writesAsMapBuilder.put(entry.getKey(), numWrites);
            }
        });
        ImmutableMap<Range<LightweightOppToken>, Long> writesAsMap = writesAsMapBuilder.build();
        log.info("The distribution of writes over token ranges for table {} is as follows: {}.",
                LoggingArgs.tableRef(tableRef),
                SafeArg.of("writesPerTokenRange", CassandraLogHelper.tokenRangesToWrites(writesAsMap)));
    }
}
