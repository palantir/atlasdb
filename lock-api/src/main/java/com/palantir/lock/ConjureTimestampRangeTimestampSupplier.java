/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.function.LongSupplier;

public final class ConjureTimestampRangeTimestampSupplier implements LongSupplier {
    private final ConjureTimestampRange range;
    private int fulfilled = 0;

    public ConjureTimestampRangeTimestampSupplier(ConjureTimestampRange range) {
        this.range = range;
    }

    @Override
    public synchronized long getAsLong() {
        if (wasExhausted()) {
            throw new SafeRuntimeException("Timestamp range has been exhausted", SafeArg.of("range", range));
        }

        long timestamp = range.getStart() + fulfilled;
        fulfilled++;
        return timestamp;
    }

    private boolean wasExhausted() {
        return fulfilled >= range.getCount();
    }
}
