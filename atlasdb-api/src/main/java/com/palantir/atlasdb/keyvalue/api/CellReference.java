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
package com.palantir.atlasdb.keyvalue.api;

import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CellReference {
    public abstract TableReference tableRef();

    public abstract Cell cell();

    /**
     * {@link Cell#hashCode()} implementation has a rather unfortunate case where it is always 0 if the row name and
     * the column name match. We did not want to change it to keep backwards compatibility, but we need a uniform
     * distribution here for all reasonable patterns.
     */
    @SuppressWarnings("EqualsHashCode") // this replaces the immutable generated code, which has equality defined
    @Override
    public int hashCode() {
        return Objects.hash(tableRef(), cell().getRowName(), cell().getColumnName());
    }

    public static CellReference of(TableReference tableRef, Cell cell) {
        return ImmutableCellReference.builder().tableRef(tableRef).cell(cell).build();
    }
}
