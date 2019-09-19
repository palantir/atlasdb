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
package com.palantir.atlasdb.schema;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.RowNamePartitioner;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Builder for a {@link TableMigrator}.
 *
 * Required arguments are srcTable, executor, checkpointer, and rangeMigrator.
 */
public class TableMigratorBuilder {
    private TableReference srcTable;
    private int partitions;
    private List<RowNamePartitioner> partitioners;
    private int readBatchSize;
    private ExecutorService executor;
    private AbstractTaskCheckpointer checkpointer;
    private TaskProgress progress;
    private ColumnSelection columnSelection;
    private RangeMigrator rangeMigrator;

    public TableMigratorBuilder() {
        srcTable = null;
        partitions = 8;
        partitioners = ImmutableList.of();
        readBatchSize = 1000;
        executor = null;
        checkpointer = null;
        progress = new NullTaskProgress();
        columnSelection = ColumnSelection.all();
        rangeMigrator = null;
    }

    public TableMigratorBuilder srcTable(TableReference table) {
        Preconditions.checkNotNull(table);
        this.srcTable = table;
        return this;
    }

    public TableMigratorBuilder partitions(int p) {
        Preconditions.checkArgument(p > 0);
        this.partitions = p;
        return this;
    }

    public TableMigratorBuilder partitioners(List<RowNamePartitioner> ps) {
        Preconditions.checkNotNull(ps);
        this.partitioners = ps;
        return this;
    }

    public TableMigratorBuilder readBatchSize(int batchSize) {
        Preconditions.checkArgument(readBatchSize > 0);
        this.readBatchSize = batchSize;
        return this;
    }

    public TableMigratorBuilder executor(ExecutorService exec) {
        Preconditions.checkNotNull(exec);
        this.executor = exec;
        return this;
    }

    public TableMigratorBuilder checkpointer(AbstractTaskCheckpointer c) {
        Preconditions.checkNotNull(c);
        this.checkpointer = c;
        return this;
    }

    public TableMigratorBuilder progress(TaskProgress p) {
        Preconditions.checkNotNull(p);
        this.progress = p;
        return this;
    }

    public TableMigratorBuilder columnSelection(ColumnSelection cs) {
        Preconditions.checkNotNull(cs);
        this.columnSelection = cs;
        return this;
    }

    public TableMigratorBuilder rangeMigrator(RangeMigrator rm) {
        Preconditions.checkNotNull(rm);
        this.rangeMigrator = rm;
        return this;
    }

    public TableMigrator build() {
        Preconditions.checkNotNull(srcTable);
        Preconditions.checkNotNull(executor);
        Preconditions.checkNotNull(checkpointer);
        Preconditions.checkNotNull(rangeMigrator);

        return new TableMigrator(
                srcTable,
                partitions,
                partitioners,
                readBatchSize,
                executor,
                checkpointer,
                progress,
                columnSelection,
                rangeMigrator);
    }
}
