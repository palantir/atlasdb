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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Arrays;

public enum CommitState {
    PENDING(CommitStateBytes.PENDING_BYTE),
    COMMITTED(CommitStateBytes.COMMITTED_BYTE);

    private final byte leadingValue;

    CommitState(byte leadingValue) {
        this.leadingValue = leadingValue;
    }

    public byte toByte() {
        return leadingValue;
    }

    public static CommitState fromByte(byte value) {
        return Arrays.stream(CommitState.values())
                .filter(state -> state.leadingValue == value)
                .findFirst()
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Unexpected byte not matching any commit states", SafeArg.of("providedByte", value)));
    }

    private interface CommitStateBytes {
        byte PENDING_BYTE = 0;
        byte COMMITTED_BYTE = 1;
    }
}
