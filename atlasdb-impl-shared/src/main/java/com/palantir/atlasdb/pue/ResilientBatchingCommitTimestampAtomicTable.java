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

package com.palantir.atlasdb.pue;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.AtomicWriteException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ResilientBatchingCommitTimestampAtomicTable implements AtomicTable<Long, Optional<Long>> {
    @Override
    public void markInProgress(Set<Long> keys) {}

    @Override
    public void updateMultiple(Map<Long, Optional<Long>> keyValues) throws AtomicWriteException {}

    @Override
    public ListenableFuture<Map<Long, Optional<Long>>> get(Iterable<Long> keys) {
        return null;
    }
}
