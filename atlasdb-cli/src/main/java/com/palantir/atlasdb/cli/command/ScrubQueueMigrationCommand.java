/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cli.command;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.Preconditions;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

@Command(name = "scrub-queue-migration",
        description = "Move the contents of the old scrub queue into the new "
                + "scrub queue. This operation is resumable.")
public class ScrubQueueMigrationCommand extends SingleBackendCommand {
    private static final byte[] DUMMY_CONTENTS = new byte[] {1};

    @Option(name = {"--truncate"},
            description = "Truncate the old scrub queue instead of moving it. "
                    + "This is fine to do if you have not been running hard "
                    + "deletes to satisfy legal requirements (a background "
                    + "sweep run will eventually clean up any cells referenced "
                    + "in the scrub queue).")
    boolean truncateOldQueue;

    @Option(name = {"--batch-size"},
            description = "Batch size for copying rows, defaults to 1000.")
    Integer batchSize;

    @Override
    public int execute(AtlasDbServices services) {
        Preconditions.checkArgument(!truncateOldQueue || batchSize == null,
                "Truncating the old scrub queue and specifying a batch size are mutually exclusive options.");
        PrintWriter output = new PrintWriter(System.out, true);
        if (truncateOldQueue) {
            services.getKeyValueService().truncateTable(AtlasDbConstants.OLD_SCRUB_TABLE);
            output.println("Truncated the old scrub queue.");
        } else {
            run(services.getKeyValueService(), output, MoreObjects.firstNonNull(batchSize, 1000));
        }
        return 0;
    }

    public static void run(KeyValueService kvs,
                           PrintWriter output,
                           int batchSize) {
        Context context = new Context(kvs, output, batchSize);

        // This potentially needs to iterate multiple times because getRange
        // only grabs the most recent timestamp for each cell, but we care
        // about every timestamp in the old scrub queue (this was the main
        // problem with the old scrub queue). Each iteration will peel off the
        // top version of each cell until the entire queue is drained.
        for (int i = 0;; i++) {
            output.println("Starting iteration " + i + " of scrub migration.");
            Stopwatch watch = Stopwatch.createStarted();
            try (ClosableIterator<RowResult<Value>> iter = kvs.getRange(
                    AtlasDbConstants.OLD_SCRUB_TABLE, RangeRequest.all(), Long.MAX_VALUE)) {
                if (!iter.hasNext()) {
                    output.println("Finished all iterations of scrub migration.");
                    break;
                }
                runOnce(context, iter);
            }
            output.println("Finished iteration " + i + " of scrub migration in "
                    + watch.elapsed(TimeUnit.SECONDS) + " seconds.");
        }
    }

    private static void runOnce(Context context,
                                ClosableIterator<RowResult<Value>> iter) {
        Multimap<Cell, Value> batchToCreate = ArrayListMultimap.create();
        Multimap<Cell, Long> batchToDelete = ArrayListMultimap.create();
        while (iter.hasNext()) {
            RowResult<Value> rowResult = iter.next();
            byte[] row = rowResult.getRowName();
            for (Entry<byte[], Value> entry : rowResult.getColumns().entrySet()) {
                byte[] col = entry.getKey();
                Value value = entry.getValue();
                long timestamp = value.getTimestamp();
                String[] tableNames = StringUtils.split(
                        PtBytes.toString(value.getContents()), AtlasDbConstants.OLD_SCRUB_TABLE_SEPARATOR_CHAR);
                for (String tableName : tableNames) {
                    byte[] tableBytes = EncodingUtils.encodeVarString(tableName);
                    byte[] newCol = EncodingUtils.add(tableBytes, col);
                    Cell newCell = Cell.create(row, newCol);
                    batchToCreate.put(newCell, Value.create(DUMMY_CONTENTS, timestamp));
                }
                batchToDelete.put(Cell.create(row, col), timestamp);
                if (batchToDelete.size() >= context.batchSize) {
                    flush(context, batchToCreate, batchToDelete);
                }
            }
        }
        if (!batchToDelete.isEmpty()) {
            flush(context, batchToCreate, batchToDelete);
        }
    }

    private static void flush(Context context,
                              Multimap<Cell, Value> batchToCreate,
                              Multimap<Cell, Long> batchToDelete) {
        context.kvs.delete(AtlasDbConstants.OLD_SCRUB_TABLE, batchToDelete);
        context.totalDeletes += batchToDelete.size();
        batchToDelete.clear();
        try {
            context.kvs.putWithTimestamps(AtlasDbConstants.SCRUB_TABLE, batchToCreate);
        } catch (KeyAlreadyExistsException e) {
            context.kvs.delete(AtlasDbConstants.SCRUB_TABLE,
                    Multimaps.transformValues(batchToCreate, Value::getTimestamp));
            context.kvs.putWithTimestamps(AtlasDbConstants.SCRUB_TABLE, batchToCreate);
        }
        context.totalPuts += batchToCreate.size();
        batchToCreate.clear();
        context.output.println("Moved " + context.totalDeletes + " cells from "
                + "the old scrub queue into " + context.totalPuts + " cells in "
                + "the new scrub queue.");
    }

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }

    private static class Context {
        final KeyValueService kvs;
        final PrintWriter output;
        final int batchSize;
        long totalPuts;
        long totalDeletes;

        Context(KeyValueService kvs, PrintWriter output, int batchSize) {
            this.kvs = kvs;
            this.output = output;
            this.batchSize = batchSize;
        }
    }
}
