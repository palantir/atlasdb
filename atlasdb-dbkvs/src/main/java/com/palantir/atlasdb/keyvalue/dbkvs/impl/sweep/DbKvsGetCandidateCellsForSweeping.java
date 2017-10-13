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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class DbKvsGetCandidateCellsForSweeping {

    private final CellTsPairLoader cellTsPairLoaderFactory;

    public DbKvsGetCandidateCellsForSweeping(CellTsPairLoader cellTsPairLoaderFactory) {
        this.cellTsPairLoaderFactory = cellTsPairLoaderFactory;
    }

    public Iterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        Iterator<List<CellTsPairInfo>> cellTsIter = cellTsPairLoaderFactory.createPageIterator(tableRef, request);
        Iterator<List<CandidateCellForSweeping>> rawIter = CandidateGroupingIterator.create(cellTsIter);
        return Iterators.filter(rawIter, page -> !page.isEmpty());
    }

}
