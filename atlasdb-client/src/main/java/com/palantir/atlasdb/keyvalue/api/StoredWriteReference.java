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

import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
public interface StoredWriteReference extends Persistable {
    Hydrator<StoredWriteReference> BYTES_HYDRATOR = ImmutableStoredWriteReference::of;

    default <T> T accept(Visitor<T> visitor) {
        byte[] data = data();
        switch (data[0]) {
            case 0: return visitor.visitTableNameAsStringBinary(data);
            case 1: return visitor.visitTableIdBinary(data);
            case '{': return visitor.visitJson(data);
            default: throw new SafeIllegalArgumentException(
                    "Data stored in targeted sweep queue was not recognised as a known data format");
        }
    }

    @Override
    default byte[] persistToBytes() {
        return data();
    }

    @Parameter
    byte[] data();

    interface Visitor<T> {
        T visitJson(byte[] ref);
        T visitTableNameAsStringBinary(byte[] ref);
        T visitTableIdBinary(byte[] ref);
    }
}
