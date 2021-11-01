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
package com.palantir.nexus.db.pool;

public abstract class ResourceSharer<R, E extends Exception> {
    private final ResourceType<R, E> type;

    private R delegate = null;
    private int refs = 0;

    public ResourceSharer(ResourceType<R, E> type) {
        this.type = type;
    }

    private synchronized void deref() throws E {
        --refs;
        if (refs > 0) {
            return;
        }
        R delegateLocal = delegate;
        delegate = null;

        // close under lock in case underlying is crappy enough to not
        // handle multiple concurrent co-existing.
        type.close(delegateLocal);
    }

    public synchronized R get() {
        if (delegate == null) {
            delegate = open();
        }
        ++refs;

        return type.closeWrapper(delegate, new ResourceOnClose<E>() {
            private boolean closed = false;

            @Override
            public synchronized void close() throws E {
                if (closed) {
                    // arggh
                    return;
                }
                closed = true;
                deref();
            }
        });
    }

    protected abstract R open();
}
