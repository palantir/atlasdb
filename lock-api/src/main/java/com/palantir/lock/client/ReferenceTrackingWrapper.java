/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.util.concurrent.atomic.AtomicInteger;

public class ReferenceTrackingWrapper<T extends AutoCloseable> implements AutoCloseable {
    private final AtomicInteger referenceCount;
    private final T delegate;

    public ReferenceTrackingWrapper(T delegate) {
        this.delegate = delegate;
        this.referenceCount = new AtomicInteger(0);
    }

    public void recordReference() {
        referenceCount.incrementAndGet();
    }

    @Override
    public synchronized void close() throws Exception {
        int updatedReferenceCount = referenceCount.decrementAndGet();
        if (updatedReferenceCount == 0) {
            delegate.close();
        }
    }
}
