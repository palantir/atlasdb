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
package com.palantir.atlasdb.sweep;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweeperservice.SweeperService;

public final class SweeperServiceImpl implements SweeperService {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SweeperService.class);
    private SpecificTableSweeperImpl specificTableSweeper;

    private SweeperServiceImpl(SpecificTableSweeperImpl specificTableSweeper) {
        this.specificTableSweeper = specificTableSweeper;
    }

    public static SweeperServiceImpl create(SpecificTableSweeperImpl specificTableSweeper) {
        return new SweeperServiceImpl(specificTableSweeper);
    }

    @Override
    public boolean sweepTable(String tableName) {
        TableReference tableRef = TableReference.createFromFullyQualifiedName(tableName);

        Preconditions.checkArgument(TableReference.isFullyQualifiedName(tableName), "Table name is not fully qualified");
        Preconditions.checkState(specificTableSweeper.getKvs().getAllTableNames().contains(tableRef), "Table requested to sweep does not exist");

        return specificTableSweeper.runOnceForTable(new TableToSweep(tableRef, null), false);
    }
}

