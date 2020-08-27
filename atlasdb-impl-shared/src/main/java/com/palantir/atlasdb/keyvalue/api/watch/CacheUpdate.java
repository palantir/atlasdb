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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.palantir.lock.watch.IdentifiedVersion;
import java.util.Optional;

final class CacheUpdate {
    static final CacheUpdate FAILED = new CacheUpdate(true, Optional.empty());

    private final boolean shouldClearCache;
    private final Optional<IdentifiedVersion> version;

    CacheUpdate(boolean shouldClearCache, Optional<IdentifiedVersion> version) {
        this.shouldClearCache = shouldClearCache;
        this.version = version;
    }

    boolean shouldClearCache() {
        return shouldClearCache;
    }

    Optional<IdentifiedVersion> getVersion() {
        return version;
    }
}
