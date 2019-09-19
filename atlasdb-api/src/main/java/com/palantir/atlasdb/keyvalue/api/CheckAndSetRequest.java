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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * A request to be supplied to KeyValueService.checkAndSet.
 * Use {@link #newCell(TableReference, Cell, byte[])} if the Cell is not yet stored,
 * and {@link #singleCell(TableReference, Cell, byte[], byte[])} otherwise.
 *
 * {@link #table()} the {@link TableReference} where the Cell is stored.
 * {@link #cell()} the {@link Cell} to update.
 * {@link #oldValue()} the existing value, or empty() if no value exists.
 * {@link #newValue()} the desired new value.
 */
@Value.Immutable
public abstract class CheckAndSetRequest {
    public abstract TableReference table();

    public abstract Cell cell();

    @Nullable
    @JsonProperty("oldValue")
    protected abstract byte[] oldValueNullable();

    @JsonIgnore
    public Optional<byte[]> oldValue() {
        return Optional.ofNullable(oldValueNullable());
    }

    public abstract byte[] newValue();

    public static CheckAndSetRequest newCell(TableReference table, Cell row, byte[] newValue) {
        return new Builder().table(table).cell(row).newValue(newValue).build();
    }

    public static CheckAndSetRequest singleCell(TableReference table, Cell cell, byte[] oldValue, byte[] newValue) {
        return new Builder()
                .table(table)
                .cell(cell)
                .oldValue(Optional.of(oldValue))
                .newValue(newValue)
                .build();
    }

    public static class Builder extends ImmutableCheckAndSetRequest.Builder {
        public Builder oldValue(Optional<byte[]> oldValue) {
            return this.oldValueNullable(oldValue.orElse(null));
        }
    }
}
