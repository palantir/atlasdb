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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

public final class InitialisingSweeperService extends ForwardingObject implements SweeperService {
    private volatile SweeperService delegate;

    private InitialisingSweeperService(SweeperService sweeperService) {
        delegate = sweeperService;
    }

    public static InitialisingSweeperService create() {
        return new InitialisingSweeperService(null);
    }

    public void initialise(SweeperService sweeperService) {
        delegate = sweeperService;
    }

    @Override
    public void sweepTable(String tableName) {
        getDelegate().sweepTable(tableName);
    }

    @Override
    public void sweepTableFromStartRow(String tableName, @Nonnull String startRow) {
        getDelegate().sweepTableFromStartRow(tableName, startRow);
    }

    @Override
    public void sweepTableFromStartRowWithBatchConfig(String tableName, @Nullable String startRow,
            @Nullable Integer maxCellTsPairsToExamine, @Nullable Integer candidateBatchSize,
            @Nullable Integer deleteBatchSize) {
        getDelegate().sweepTableFromStartRowWithBatchConfig(tableName, startRow, maxCellTsPairsToExamine,
                candidateBatchSize, deleteBatchSize);
    }

    private SweeperService getDelegate() {
        return (SweeperService) delegate();
    }

    @Override
    protected Object delegate() {
        checkInitialised();
        return delegate;
    }

    void checkInitialised() {
        if (delegate == null) {
            throw new IllegalStateException("Not initialised");
        }
    }
}
