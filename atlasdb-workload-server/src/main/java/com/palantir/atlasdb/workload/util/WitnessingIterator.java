/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.util;

import java.util.Iterator;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class WitnessingIterator<T> implements Iterator<T> {
    private final UUID iteratorIdentifier;
    private final Iterator<T> delegate;
    private final BiConsumer<UUID, T> readWitnessCallback;
    private final Consumer<UUID> exhaustionWitnessCallback;

    public WitnessingIterator(
            UUID iteratorIdentifier,
            Iterator<T> delegate,
            BiConsumer<UUID, T> readWitnessCallback,
            Consumer<UUID> exhaustionWitnessCallback) {
        this.iteratorIdentifier = iteratorIdentifier;
        this.delegate = delegate;
        this.readWitnessCallback = readWitnessCallback;
        this.exhaustionWitnessCallback = exhaustionWitnessCallback;
    }

    @Override
    public boolean hasNext() {
        if (delegate.hasNext()) {
            return true;
        }
        exhaustionWitnessCallback.accept(iteratorIdentifier);
        return false;
    }

    @Override
    public T next() {
        T next = delegate.next();
        readWitnessCallback.accept(iteratorIdentifier, next);
        return next;
    }
}
