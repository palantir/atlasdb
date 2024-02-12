/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import java.util.function.Predicate;
import org.immutables.value.Value;

/**
 * Indicates how the table mapping cache should be configured. This is to account for dynamic tables, for which caching
 * table mapping lookups may be unsafe.
 */
@Value.Immutable(singleton = true)
public interface TableMappingCacheConfiguration {
    /**
     * A predicate indicating whether the table mapping for these *fully qualified* table names should be cached.
     * Users with dynamic tables that may be deleted and re-created should treat these tables as non-cacheable, so that
     * the latest table mapping will be loaded from the database on each pertinent query.
     */
    @Value.Default
    default Predicate<TableReference> cacheableTablePredicate() {
        return table -> true;
    }
}
