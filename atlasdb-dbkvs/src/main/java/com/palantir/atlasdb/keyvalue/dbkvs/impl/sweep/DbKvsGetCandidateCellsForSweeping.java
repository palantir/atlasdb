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
import java.util.Optional;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;

public class DbKvsGetCandidateCellsForSweeping {

    private final CellTsPairLoaderFactory cellTsPairLoaderFactory;

    public DbKvsGetCandidateCellsForSweeping(CellTsPairLoaderFactory cellTsPairLoaderFactory) {
        this.cellTsPairLoaderFactory = cellTsPairLoaderFactory;
    }

    public Iterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        CellTsPairLoader loader = cellTsPairLoaderFactory.createCellTsLoader(tableRef, request);
        CandidatePagingState state = CandidatePagingState.create(request.startRowInclusive());
        return Iterators.filter(new PageIterator(loader, state), page -> !page.isEmpty());
    }

    private class PageIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final CellTsPairLoader pageLoader;
        private final CandidatePagingState state;

        PageIterator(CellTsPairLoader pageLoader, CandidatePagingState state) {
            this.pageLoader = pageLoader;
            this.state = state;
        }

        @Override
        protected List<CandidateCellForSweeping> computeNext() {
            Optional<StartingPosition> startingPosition = state.getNextStartingPosition();
            if (startingPosition.isPresent()) {
                return loadNextPage(startingPosition.get());
            } else {
                return endOfData();
            }
        }

        private List<CandidateCellForSweeping> loadNextPage(StartingPosition startingPosition) {
            CellTsPairLoader.Page page = pageLoader.loadNextPage(
                    startingPosition, state.getCellTsPairsExaminedInCurrentRow());
            List<CandidateCellForSweeping> candidates = state.processBatch(
                    page.entries, page.reachedEnd);
            if (page.reachedEnd && page.scannedSingleRow) {
                // If we reached the end of results in the single-row mode, it doesn't mean that we reached
                // the end of the table. We need to restart from the next row in the normal mode.
                state.restartFromNextRow();
            }
            return candidates;
        }
    }
}
