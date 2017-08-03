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

package com.palantir.atlasdb.deepkin;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;

public class CachedBatchingVisitable<T extends Serializable> implements BatchingVisitable<T>, Serializable {
    private LinkedList<T> queue;

    private CachedBatchingVisitable(LinkedList<T> queue) {
        this.queue = queue;
    }

    @Override
    public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> v) throws K {
        while (!queue.isEmpty()) {
            List<T> batch = queue.subList(0, batchSize);
            IntStream.range(0, batchSize).forEach(i -> queue.poll());
            if (!v.visit(batch)) {
                return false;
            }
        }
        return true;
    }

    public static <T extends Serializable> CachedBatchingVisitable<T> cache(BatchingVisitable<T> delegate) {
        LinkedList<T> queue = Lists.newLinkedList();
        delegate.batchAccept(100, queue::addAll);
        return new CachedBatchingVisitable<>(queue);
    }
}
