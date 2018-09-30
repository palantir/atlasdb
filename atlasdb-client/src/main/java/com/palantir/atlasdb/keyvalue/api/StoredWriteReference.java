/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import com.palantir.common.persist.Persistable;

@Immutable
public interface StoredWriteReference extends Persistable {
    Hydrator<StoredWriteReference> BYTES_HYDRATOR = ImmutableStoredWriteReference::of;

    default <T> T accept(Visitor<T> visitor) {
        byte[] data = data();
        switch (data[0]) {
            case 0: return visitor.visitTableNameAsStringBinary(data);
            case 1: return visitor.visitTableIdBinary(data);
            default: return visitor.visitJson(data);
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
