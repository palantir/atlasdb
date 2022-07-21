/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import java.util.Set;

/**
 * Dummy impl since we do not have `AbortedTimestampStore` yet.
 * */
public final class AbortedTimestampStore {

    public Set<Long> getBucket(long bucket) {
        return null;
    }

    public void addAbortedTimestamps(Set<Long> abortedTimestamps) {}

    public Set<Long> getAbortedTransactionsInRange(long startInclusive, long endInclusive) {
        return null;
    }
}
