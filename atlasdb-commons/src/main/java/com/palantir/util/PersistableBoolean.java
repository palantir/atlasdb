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

package com.palantir.util;

import java.util.Arrays;

import org.immutables.value.Value;

import com.palantir.common.persist.Persistable;

@Value.Immutable
public abstract class PersistableBoolean implements Persistable {
    public abstract boolean value();

    private static final byte[] FALSE = new byte[]{0};
    private static final byte[] NOT_FALSE = new byte[]{1};

    public static final Hydrator<PersistableBoolean> BYTES_HYDRATOR = input -> PersistableBoolean.of(!Arrays.equals(input, FALSE));

    @Override
    public byte[] persistToBytes() {
        return value() ? NOT_FALSE : FALSE;
    }

    public static PersistableBoolean of(boolean value) {
        return ImmutablePersistableBoolean.builder().value(value).build();
    }
}
