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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;

public class CandidatePageJoiningIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
    private final Iterator<List<CandidateCellForSweeping>> sourceIterator;
    private final int minPageSize;

    public CandidatePageJoiningIterator(Iterator<List<CandidateCellForSweeping>> sourceIterator, int minPageSize) {
        this.sourceIterator = sourceIterator;
        this.minPageSize = minPageSize;
    }

    @Override
    protected List<CandidateCellForSweeping> computeNext() {
        if (!sourceIterator.hasNext()) {
            return endOfData();
        } else {
            List<CandidateCellForSweeping> page = sourceIterator.next();
            int pageSize = getSizeOfPage(page);
            if (pageSize >= minPageSize) {
                return page;
            } else {
                List<CandidateCellForSweeping> ret = Lists.newArrayList();
                int retSize = pageSize;
                ret.addAll(page);
                // The order is important: calling hasNext() is expensive,
                // so we should check first if we reached the desired page size
                while (retSize < minPageSize && sourceIterator.hasNext()) {
                    List<CandidateCellForSweeping> nextPage = sourceIterator.next();
                    retSize += getSizeOfPage(nextPage);
                    ret.addAll(nextPage);
                }
                return ret.isEmpty() ? endOfData() : ret;
            }
        }
    }

    private static int getSizeOfPage(List<CandidateCellForSweeping> page) {
        // If a cell doesn't have any timestamps (i.e., is not a candidate), we don't want count
        // it as zero because it still takes memory. Hence max(1, ...).
        return page.stream().mapToInt(c -> Math.max(1, c.sortedTimestamps().length)).sum();
    }
}
