/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Optional;

import org.immutables.value.Value;

import com.palantir.atlasdb.keyvalue.api.TableReference;

// Union type of RowReference and RowPrefixReference
@Value.Immutable
public interface RowCacheReference {
    Optional<RowReference> rowReference();
    Optional<RowPrefixReference> prefixReference();

    @Value.Lazy
    default TableReference tableReference() {
        return rowReference().map(RowReference::tableReference)
                .orElseGet(() -> prefixReference().get().tableReference());
    }

    static RowCacheReference ofRowReference(RowReference rowReference) {
        return ImmutableRowCacheReference.builder().rowReference(rowReference).build();
    }

    static RowCacheReference ofPrefixReference(RowPrefixReference prefixReference) {
        return ImmutableRowCacheReference.builder().prefixReference(prefixReference).build();
    }
}
