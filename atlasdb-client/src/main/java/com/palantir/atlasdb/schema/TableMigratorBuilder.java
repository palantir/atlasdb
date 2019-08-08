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

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.RowNamePartitioner;

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
        com.palantir.logsafe.Preconditions.checkNotNull(table);
        this.srcTable = table;
        return this;
    }

    public TableMigratorBuilder partitions(int p) {
        com.palantir.logsafe.Preconditions.checkArgument(p > 0);
        this.partitions = p;
        return this;
    }

    public TableMigratorBuilder partitioners(List<RowNamePartitioner> ps) {
        com.palantir.logsafe.Preconditions.checkNotNull(ps);
        this.partitioners = ps;
        return this;
    }

    public TableMigratorBuilder readBatchSize(int batchSize) {
        com.palantir.logsafe.Preconditions.checkArgument(readBatchSize > 0);
        this.readBatchSize = batchSize;
        return this;
    }

    public TableMigratorBuilder executor(ExecutorService exec) {
        com.palantir.logsafe.Preconditions.checkNotNull(exec);
        this.executor = exec;
        return this;
    }

    public TableMigratorBuilder checkpointer(AbstractTaskCheckpointer c) {
        com.palantir.logsafe.Preconditions.checkNotNull(c);
        this.checkpointer = c;
        return this;
    }

    public TableMigratorBuilder progress(TaskProgress p) {
        com.palantir.logsafe.Preconditions.checkNotNull(p);
        this.progress = p;
        return this;
    }

    public TableMigratorBuilder columnSelection(ColumnSelection cs) {
        com.palantir.logsafe.Preconditions.checkNotNull(cs);
        this.columnSelection = cs;
        return this;
    }

    public TableMigratorBuilder rangeMigrator(RangeMigrator rm) {
        com.palantir.logsafe.Preconditions.checkNotNull(rm);
        this.rangeMigrator = rm;
        return this;
    }

    public TableMigrator build() {
        com.palantir.logsafe.Preconditions.checkNotNull(srcTable);
        com.palantir.logsafe.Preconditions.checkNotNull(executor);
        com.palantir.logsafe.Preconditions.checkNotNull(checkpointer);
        com.palantir.logsafe.Preconditions.checkNotNull(rangeMigrator);

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
