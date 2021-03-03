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

import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.watch.LockWatchEventCache;
import java.util.Optional;

public class BatchingCommitTimestampGetterFactory {
    private final LockWatchEventCache cache;
    private final Optional<Namespace> namespace;
    private final Optional<MultiClientCommitTimestampGetter> maybeBatcher;

    public BatchingCommitTimestampGetterFactory(
            LockWatchEventCache cache,
            Optional<Namespace> namespace,
            Optional<MultiClientCommitTimestampGetter> maybeBatcher) {
        this.cache = cache;
        this.namespace = namespace;
        this.maybeBatcher = maybeBatcher;
    }

    public CommitTimestampGetter get(LockLeaseService lockLeaseService) {
        if (maybeBatcher.isPresent() && namespace.isPresent()) {
            return new NamespacedCommitTimestampGetter(cache, namespace.get(), maybeBatcher.get());
        }
        return BatchingCommitTimestampGetter.create(lockLeaseService, cache);
    }
}
