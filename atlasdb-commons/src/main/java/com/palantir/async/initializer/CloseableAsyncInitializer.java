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

package com.palantir.async.initializer;

import com.palantir.exception.NotInitializedException;

public abstract class CloseableAsyncInitializer<T extends AutoCloseable> extends AsyncInitializer
        implements AutoCloseable {
    enum Status {
        OPEN, CLOSING, CLOSED
    }

    private volatile Status status = Status.OPEN;

    public T delegate() {
        if (status == Status.CLOSING) {
            close();

            if (status != Status.CLOSED) {
                throw new NotInitializedException("CassandraKeyValueService");
            }
        }

        return delegateInternal();
    }

    protected abstract T delegateInternal();

    @Override
    public void close() {
        if (status != Status.CLOSED) {
            try {
                delegateInternal().close();
                status = Status.CLOSED;
            } catch (NotInitializedException e) {
                // The wrapper is closed, but we still have to propagate this to the delegate, once it initializes.
                status = Status.CLOSING;
            } catch (Exception e) {
                throw new RuntimeException("", e);
            }
        }
    }
}
