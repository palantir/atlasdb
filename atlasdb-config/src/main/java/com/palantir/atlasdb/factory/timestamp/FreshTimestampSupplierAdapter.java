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
package com.palantir.atlasdb.factory.timestamp;

import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.Preconditions;
import com.palantir.timestamp.TimestampService;
import java.util.function.LongSupplier;

public class FreshTimestampSupplierAdapter implements LongSupplier {
    private volatile TimestampService timestampService;

    public void setTimestampService(TimestampService timestampService) {
        Preconditions.checkNotNull(timestampService, "Should not re-set timestamp service in a"
                + " FreshTimestampSupplierAdapter to null");
        this.timestampService = timestampService;
    }

    @Override
    public long getAsLong() {
        if (timestampService == null) {
            throwNotInitializedException();
        }
        return timestampService.getFreshTimestamp();
    }

    private static long throwNotInitializedException() {
        throw new NotInitializedException(FreshTimestampSupplierAdapter.class.getName());
    }
}
