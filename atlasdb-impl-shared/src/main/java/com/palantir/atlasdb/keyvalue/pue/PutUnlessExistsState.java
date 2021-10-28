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

package com.palantir.atlasdb.keyvalue.pue;

import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.conjure.java.lib.Bytes;
import org.immutables.value.Value;

@Value.Immutable
public interface PutUnlessExistsState {
    Bytes value();

    CommitState commitState();

    @Value.Lazy
    default byte[] toByteArray() {
        return EncodingUtils.add(new byte[] {commitState().toByte()}, value().asNewByteArray());
    }

    static PutUnlessExistsState fromBytes(byte[] array) {
        return ImmutablePutUnlessExistsState.builder()
                .commitState(CommitState.fromByte(array[0]))
                .value(Bytes.from(array, 1, array.length - 1))
                .build();
    }
}
