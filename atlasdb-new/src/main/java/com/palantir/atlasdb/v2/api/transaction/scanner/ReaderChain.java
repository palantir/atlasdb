/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import java.util.function.Function;

import com.palantir.atlasdb.v2.api.api.NewValue;

public final class ReaderChain<E extends NewValue, R extends Reader<E>> {
    private final R reader;

    private ReaderChain(R reader) {
        this.reader = reader;
    }

    public static <E extends NewValue, R extends Reader<E>> ReaderChain<E, R> create(R base) {
        return new ReaderChain<>(base);
    }

    public <U extends NewValue, R1 extends Reader<U>> ReaderChain<U, R1> then(Function<R, R1> factory) {
        return new ReaderChain<>(factory.apply(reader));
    }

    public R build() {
        return reader;
    }
}
