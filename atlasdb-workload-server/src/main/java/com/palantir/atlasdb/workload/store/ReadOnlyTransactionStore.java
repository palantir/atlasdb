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

package com.palantir.atlasdb.workload.store;

import java.util.Optional;

/**
 * Facade around {@link ReadableTransactionStore}, to prevent users from casting to retrieve an underlying store that
 * may implement more functionality.
 */
public final class ReadOnlyTransactionStore implements ReadableTransactionStore {
    private final ReadableTransactionStore delegate;

    public ReadOnlyTransactionStore(ReadableTransactionStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<Integer> get(String table, WorkloadCell cell) {
        return delegate.get(table, cell);
    }
}
