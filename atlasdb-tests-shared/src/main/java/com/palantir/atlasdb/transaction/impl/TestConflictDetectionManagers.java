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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.Map;

public final class TestConflictDetectionManagers {
    private TestConflictDetectionManagers() {}

    @VisibleForTesting
    static ConflictDetectionManager createWithStaticConflictDetection(Map<TableReference, ConflictHandler> staticMap) {
        return new ConflictDetectionManager(new CacheLoader<TableReference, ConflictHandler>() {
            @Override
            public ConflictHandler load(TableReference tableReference) throws Exception {
                return staticMap.getOrDefault(tableReference, ConflictHandler.RETRY_ON_WRITE_WRITE);
            }
        });
    }
}
