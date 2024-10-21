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

package com.palantir.lock.client.timestampleases;

import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.function.LongSupplier;

final class ExactTimestampSupplier implements LongSupplier {
    private static final SafeLogger log = SafeLoggerFactory.get(ExactTimestampSupplier.class);

    private final ConjureTimestampRange range;
    private final int requested;

    private int fulfilled = 0;

    private ExactTimestampSupplier(ConjureTimestampRange range, int requested) {
        this.range = range;
        this.requested = requested;
    }

    static ExactTimestampSupplier create(ConjureTimestampRange range, int requested) {
        if (range.getCount() < requested) {
            log.error(
                    "The range provided does not contain the requested count of timestamps",
                    SafeArg.of("requested", requested),
                    SafeArg.of("range", range));
            throw new SafeIllegalArgumentException(
                    "The range provided does not contain the requested count of timestamps");
        }
        return new ExactTimestampSupplier(range, requested);
    }

    @Override
    public synchronized long getAsLong() {
        if (fulfilled == requested) {
            log.error(
                    "Trying to fetch more timestamps than initially requested",
                    SafeArg.of("requested", requested),
                    SafeArg.of("fulfilled", fulfilled),
                    SafeArg.of("range", range));
            throw new SafeIllegalStateException("Trying to fetch more timestamps than initially requested");
        }

        long timestamp = range.getStart() + fulfilled;
        fulfilled++;
        return timestamp;
    }
}
