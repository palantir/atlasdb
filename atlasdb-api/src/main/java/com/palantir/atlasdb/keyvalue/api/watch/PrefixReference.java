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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.util.Optional;

import org.immutables.value.Value;

import com.palantir.atlasdb.keyvalue.api.TableReference;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public interface PrefixReference {
    TableReference tableRef();
    Optional<byte[]> prefix();

    static PrefixReference prefix(TableReference tableRef, byte[] prefix) {
        return ImmutablePrefixReference.builder().tableRef(tableRef).prefix(prefix).build();
    }

    static PrefixReference table(TableReference tableRef) {
        return ImmutablePrefixReference.builder()
                .tableRef(tableRef)
                .build();
    }
}
