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

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;

// The batches can be very big at the start, so we should re-partition them by deleteBatchSize at the start.
public class CandidateCellForSweepingPartitioningIterator extends
        AbstractIterator<List<CandidateCellForSweeping>> {

    private int deleteBatchSize;
    private Iterator<List<CandidateCellForSweeping>> candidates;
    private List<CandidateCellForSweeping> previousBatch;

    protected CandidateCellForSweepingPartitioningIterator(
            Iterator<List<CandidateCellForSweeping>> candidates, int deleteBatchSize) {
        Preconditions.checkArgument(deleteBatchSize > 0, "Iterator batch size must be positive");

        this.deleteBatchSize = deleteBatchSize;
        this.candidates = candidates;
        this.previousBatch = Lists.newArrayList();
    }

    @Override
    protected List<CandidateCellForSweeping> computeNext() {
        if (!candidates.hasNext() && previousBatch.isEmpty()) {
            return endOfData();
        }

        List<CandidateCellForSweeping> currentBatch = Lists.newArrayList();
        MutableInt numCellTsExamined = new MutableInt(0);
        previousBatch = moveCells(previousBatch, currentBatch, numCellTsExamined);

        while (candidates.hasNext() && numCellTsExamined.getValue() < deleteBatchSize) {
            List<CandidateCellForSweeping> currentCandidates = candidates.next();
            previousBatch = moveCells(currentCandidates, currentBatch, numCellTsExamined);
        }

        return currentBatch;
    }

    // Move cells from the readBatch to the writeBatch for at most currentNumCellsAnalyzed.
    private List<CandidateCellForSweeping> moveCells(List<CandidateCellForSweeping> readBatch,
            List<CandidateCellForSweeping> writeBatch, MutableInt numCellTsExamined) {

        int currentNumCellTsExamined = numCellTsExamined.getValue();

        int readBatchIndex = 0;
        for (; readBatchIndex < readBatch.size() && currentNumCellTsExamined < deleteBatchSize; readBatchIndex++) {
            currentNumCellTsExamined += readBatch.get(readBatchIndex).numCellsTsPairsExamined();
        }

        numCellTsExamined.setValue(currentNumCellTsExamined);
        writeBatch.addAll(readBatch.subList(0, readBatchIndex));

        if (readBatchIndex == readBatch.size()) {
            return Lists.newArrayList();
        }
        return readBatch.subList(readBatchIndex, readBatch.size());
    }
}
