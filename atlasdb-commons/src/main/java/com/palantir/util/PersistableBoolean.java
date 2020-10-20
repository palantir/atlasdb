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
package com.palantir.util;

import com.palantir.common.persist.Persistable;
import java.util.Arrays;

public enum PersistableBoolean implements Persistable {
    TRUE {
        @Override
        public byte[] persistToBytes() {
            return ONE;
        }
    },
    FALSE {
        @Override
        public byte[] persistToBytes() {
            return ZERO;
        }
    };

    private static final byte[] ONE = new byte[] {1};
    private static final byte[] ZERO = new byte[] {0};

    /**
     *  returns FALSE iff the provided byte array is a single zero byte. Otherwise returns TRUE
     */
    public static final Hydrator<PersistableBoolean> BYTES_HYDRATOR =
            val -> PersistableBoolean.of(Arrays.equals(val, ZERO));

    public static PersistableBoolean of(boolean value) {
        return value ? TRUE : FALSE;
    }

    public boolean isTrue() {
        return this == TRUE;
    }
}
