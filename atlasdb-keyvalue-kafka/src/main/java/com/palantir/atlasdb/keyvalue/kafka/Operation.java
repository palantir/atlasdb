/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

public class Operation<R> {
    private final Tag tag;
    private final AtomicReference<CompletableFuture<R>> result = new AtomicReference<>(null);

    public Operation(Tag tag) {
        this.tag = tag;
    }

    public Tag getTag() {
        return tag;
    }

    public synchronized void setResult(CompletableFuture<R> newResult) {
        boolean casResult = result.compareAndSet(null, newResult);
        Preconditions.checkState(casResult, "Result has already been set!");

        this.notifyAll();
    }

    public synchronized R getResult() {
        while (result.get() == null) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
        return Futures.getUnchecked(result.get());
    }

    public R getResultWithKeyAlreadyExistsException() throws KeyAlreadyExistsException {
        try {
            return getResult();
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof KeyAlreadyExistsException) {
                throw (KeyAlreadyExistsException) e.getCause();
            }
            throw Throwables.propagate(e);
        }
    }
}
