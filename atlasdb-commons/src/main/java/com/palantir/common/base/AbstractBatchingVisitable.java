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
package com.palantir.common.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Collections;
import java.util.List;

/**
 * This abstract class will implement the required methods in {@link BatchingVisitable}
 * and will also implement the requires batchSize guarantee (only the last page is allowed to be
 * smaller than the batch size).
 */
public abstract class AbstractBatchingVisitable<T> implements BatchingVisitable<T> {
    @Override
    final public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> v) throws K {
        Preconditions.checkArgument(batchSize > 0);
        if (v instanceof ConsistentVisitor) {
            @SuppressWarnings("unchecked")
            AbortingVisitor<List<T>, K> v2 = (AbortingVisitor<List<T>, K>) v;
            ConsistentVisitor<T, K> consistentVisitor = (ConsistentVisitor<T, K>) v2;
            Preconditions.checkState(consistentVisitor.visitorAlwaysReturnedTrue,
                    "passed a visitor that has already said stop");
            batchAcceptSizeHint(batchSize, consistentVisitor);
            return consistentVisitor.visitorAlwaysReturnedTrue;
        }
        AbstractBatchingVisitable.ConsistentVisitor<T, K> consistentVisitor = AbstractBatchingVisitable.ConsistentVisitor.create(batchSize, v);
        batchAcceptSizeHint(batchSize, consistentVisitor);
        if (consistentVisitor.visitorAlwaysReturnedTrue && !consistentVisitor.buffer.isEmpty()) {
            Preconditions.checkState(consistentVisitor.buffer.size() < batchSize);
            return v.visit(Collections.unmodifiableList(consistentVisitor.buffer));
        } else {
            return consistentVisitor.visitorAlwaysReturnedTrue;
        }
    }

    /**
     * The batch size passed to this method is purely a hint.
     * The underlying impl can batch up pages however it wants
     * and pass them to the visitor.  Batch size consistency is already taken care of by the
     * {@link ConsistentVisitor}.
     */
    protected abstract <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                         ConsistentVisitor<T, K> v) throws K;

    protected final static class ConsistentVisitor<T, K extends Exception> implements AbortingVisitor<List<T>, K> {
        final int batchSize;
        final AbortingVisitor<? super List<T>, K> v;
        List<T> buffer = Lists.newArrayList();
        boolean visitorAlwaysReturnedTrue = true;

        private ConsistentVisitor(int batchSize, AbortingVisitor<? super List<T>, K> av) {
            Preconditions.checkArgument(batchSize > 0);
            this.batchSize = batchSize;
            this.v = Preconditions.checkNotNull(av);
        }

        static <T, K extends Exception> ConsistentVisitor<T, K> create(int batchSize,
                AbortingVisitor<? super List<T>, K> v) {
            return new ConsistentVisitor<T, K>(batchSize, v);
        }

        public boolean visitOne(T t) throws K {
            return visit(ImmutableList.of(t));
        }

        @Override
        public boolean visit(List<T> list) throws K {
            if (!visitorAlwaysReturnedTrue) {
                throw new SafeIllegalStateException("Cannot keep visiting if visitor returns false.");
            }

            if (buffer.isEmpty() && list.size() == batchSize) {
                // Special case: We have exactly one batch.
                return visitBufferWithDelegate(Collections.unmodifiableList(list));
            }

            buffer.addAll(list);
            if (buffer.size() < batchSize) {
                return true;
            }
            return processBufferBatches();
        }

        private boolean visitBufferWithDelegate(List<T> list) throws K {
            boolean ret = v.visit(list);
            visitorAlwaysReturnedTrue &= ret;
            return ret;
        }

        private boolean processBufferBatches() throws K {
            List<List<T>> batches = Lists.partition(buffer, batchSize);
            for (List<T> batch : batches) {
                if (batch.size() != batchSize) {
                    continue;
                }
                if (!visitBufferWithDelegate(Collections.unmodifiableList(batch))) {
                    return false;
                }
            }
            List<T> lastBatch = batches.get(batches.size()-1);
            if (lastBatch.size() == batchSize) {
                buffer = Lists.newArrayList();
            } else {
                buffer = Lists.newArrayList(lastBatch);
            }
            return true;
        }
    }
}
