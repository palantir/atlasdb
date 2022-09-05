/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.table.description;

import com.palantir.common.persist.Persistable;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Arrays;
import java.util.stream.Stream;

public enum SweeperStrategy implements Persistable {
    CONSERVATIVE {
        @Override
        public byte[] persistToBytes() {
            return CONS;
        }
    },
    THOROUGH {
        @Override
        public byte[] persistToBytes() {
            return THOR;
        }
    },
    NON_SWEEPABLE {
        @Override
        public byte[] persistToBytes() {
            return NONE;
        }
    };

    private static final byte[] CONS = new byte[] {1};
    private static final byte[] THOR = new byte[] {0};
    private static final byte[] NONE = new byte[] {2};

    public static final Hydrator<SweeperStrategy> BYTES_HYDRATOR =
            val -> KeyedStream.of(Stream.of(SweeperStrategy.values()))
                    .map(SweeperStrategy::persistToBytes)
                    .filter(bytes -> Arrays.equals(val, bytes))
                    .keys()
                    .findFirst()
                    .orElseThrow(() -> new SafeIllegalArgumentException(
                            "Unknown sweeper strategy", SafeArg.of("bytes", Arrays.toString(val))));
}
