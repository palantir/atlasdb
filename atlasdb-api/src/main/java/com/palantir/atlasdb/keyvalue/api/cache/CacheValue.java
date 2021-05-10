/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import java.util.Arrays;
import java.util.Optional;

public final class CacheValue {
    private final Optional<byte[]> value;

    private CacheValue(Optional<byte[]> value) {
        this.value = value;
    }

    // This is optional as we also want to cache reads where there is no value present.
    public Optional<byte[]> value() {
        return value;
    }

    public static CacheValue of(byte[] value) {
        return new CacheValue(Optional.of(value));
    }

    public static CacheValue empty() {
        return new CacheValue((Optional.empty()));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CacheValue)) {
            return false;
        }

        CacheValue other = (CacheValue) obj;

        if (value().isPresent()) {
            if (other.value().isPresent()) {
                return Arrays.equals(value().get(), other.value().get());
            } else {
                return false;
            }
        } else {
            return !other.value().isPresent();
        }
    }

    @Override
    public int hashCode() {
        // Optionals do return 0 for empty values, but the hash code uses Object, which bases array hash codes on the
        // reference, not the values.
        return value.map(Arrays::hashCode).orElse(0);
    }
}
