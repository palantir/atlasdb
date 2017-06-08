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

public class PageJoiningIterator<T> extends AbstractIterator<List<T>> {
    private final Iterator<List<T>> sourceIterator;
    private final int minPageSize;

    public PageJoiningIterator(Iterator<List<T>> sourceIterator, int minPageSize) {
        this.sourceIterator = sourceIterator;
        this.minPageSize = minPageSize;
    }

    @Override
    protected List<T> computeNext() {
        if (!sourceIterator.hasNext()) {
            return endOfData();
        } else {
            List<T> page = sourceIterator.next();
            if (page.size() >= minPageSize) {
                return page;
            } else {
                List<T> ret = Lists.newArrayList();
                ret.addAll(page);
                // The order is important: calling hasNext() is expensive,
                // so we should check first if we reached the desired page size
                while (ret.size() < minPageSize && sourceIterator.hasNext()) {
                    ret.addAll(sourceIterator.next());
                }
                return ret.isEmpty() ? endOfData() : ret;
            }
        }
    }
}
