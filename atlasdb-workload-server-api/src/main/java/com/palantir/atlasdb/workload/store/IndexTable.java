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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import org.immutables.value.Value;

@Value.Immutable
public interface IndexTable {

    @Value.Parameter
    String name();

    @Value.Parameter
    String primaryTable();

    @Value.Check
    default void checkNameAndPrimaryTableAreNotEqual() {
        Preconditions.checkArgument(
                !name().equalsIgnoreCase(primaryTable()),
                "Index table name and primary table name are identical.",
                SafeArg.of("name", name()),
                SafeArg.of("primaryTable", primaryTable()));
    }

    static IndexTable of(String name, String primaryTable) {
        return ImmutableIndexTable.of(name, primaryTable);
    }
}
