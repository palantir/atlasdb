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

import com.google.common.collect.Iterables;
import com.palantir.common.collect.IterableUtils;
import com.palantir.common.collect.IteratorUtils;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class BatchingVisitableFromIterable<T> extends AbstractBatchingVisitable<T> {

    private static final int MAX_BATCH_SIZE = 10000;

    private final Iterable<T> iterable;

    public static <T> BatchingVisitable<T> create(Iterable<? extends T> iterable) {
        return new BatchingVisitableFromIterable<T>(iterable);
    }

    public BatchingVisitableFromIterable(Iterable<? extends T> iterable) {
        this.iterable = IterableUtils.wrap(iterable);
    }

    /**
     * This creates a one time use visitable.  The only proper use of the returned class is to call
     * accept on it directly and never reference it again.
     */
    public static <T> BatchingVisitable<T> create(final Iterator<? extends T> iterator) {
        return new SingleCallBatchingVisitable<>(create(() -> IteratorUtils.wrap(iterator)));
    }

    @Override
    public <K extends Exception> void batchAcceptSizeHint(int batchSize, ConsistentVisitor<T, K> v) throws K {
        /*
         * Iterables.partition allocates an array of size batchSize, so avoid an OOM by making sure
         * it's not too big.
         */
        batchSize = Math.min(batchSize, MAX_BATCH_SIZE);
        for (List<T> list : Iterables.partition(iterable, batchSize)) {
            if (!v.visit(list)) {
                return;
            }
        }
    }

    private static final class SingleCallBatchingVisitable<T> implements BatchingVisitable<T> {

        @SuppressWarnings("rawtypes")
        private static final AtomicReferenceFieldUpdater<SingleCallBatchingVisitable, BatchingVisitable>
                delegateUpdater = AtomicReferenceFieldUpdater.newUpdater(
                        SingleCallBatchingVisitable.class, BatchingVisitable.class, "delegate");

        private volatile BatchingVisitable<T> delegate;

        SingleCallBatchingVisitable(BatchingVisitable<T> delegate) {
            this.delegate = Preconditions.checkNotNull(delegate, "Delegate BatchingVisitable is required");
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> visitor)
                throws K {
            BatchingVisitable<T> current = (BatchingVisitable<T>) delegateUpdater.getAndSet(this, null);
            if (current == null) {
                throw new SafeIllegalStateException("This class has already been called once before");
            }
            return current.batchAccept(batchSize, visitor);
        }

        @Override
        public String toString() {
            return "SingleCallBatchingVisitable{" + delegate + '}';
        }
    }
}
