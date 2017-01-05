/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.api;

import org.immutables.value.Value;

@Value.Immutable
public abstract class CheckAndSetRequest {
    public abstract TableReference table();

    public abstract Cell row();

    @Value.Default
    public byte[] oldValue() {
        return new byte[0];
    }

    public abstract byte[] newValue();

    public static CheckAndSetRequest newCell(TableReference table, Cell row, byte[] newValue) {
        return ImmutableCheckAndSetRequest.builder().table(table).row(row).newValue(newValue).build();
    }

    public static CheckAndSetRequest singleCell(TableReference table, Cell cell, byte[] oldValue, byte[] newValue) {
        return ImmutableCheckAndSetRequest.builder()
                .table(table)
                .row(cell)
                .oldValue(oldValue)
                .newValue(newValue)
                .build();
    }
}
