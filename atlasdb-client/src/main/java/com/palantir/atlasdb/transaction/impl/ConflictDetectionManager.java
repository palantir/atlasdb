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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.Map;
import javax.annotation.Nullable;

public class ConflictDetectionManager {
    private final LoadingCache<TableReference, ConflictHandler> cache;

    /**
     *  This class does not make the mistake of attempting cache invalidation,
     *  so a table dropped by another instance may still be cached here.
     *
     *  This is okay in the case of a simple drop, but a same-name table drop
     *  and re-addition with a different Conflict Handler
     *  (where an external atlas instance handles both of these operations)
     *  will be incorrect. This is an unrealistic workflow
     *  and I'm fine with just documenting this behavior.
     *
     *  (This has always been the behavior of this class; I'm simply calling it out)
     */
    public ConflictDetectionManager(CacheLoader<TableReference, ConflictHandler> loader) {
        this.cache = CacheBuilder.newBuilder().maximumSize(100_000).build(loader);
    }

    public void warmCacheWith(Map<TableReference, ConflictHandler> preload) {
        cache.putAll(preload);
    }

    public Map<TableReference, ConflictHandler> getCachedValues() {
        return cache.asMap();
    }

    @Nullable
    public ConflictHandler get(TableReference tableReference) {
        return cache.getUnchecked(tableReference);
    }
}
