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

package com.palantir.atlasdb.factory.timestamp;

import java.util.function.LongSupplier;

import com.palantir.exception.NotInitializedException;
import com.palantir.timestamp.TimestampService;

public class FreshTimestampSupplierAdapter implements LongSupplier {
    public static LongSupplier NO_TIMESTAMP_SERVICE = FreshTimestampSupplierAdapter::throwNotInitializedException;

    private volatile TimestampService timestampService;

    public void setTimestampService(TimestampService timestampService) {
        this.timestampService = timestampService;
    }

    @Override
    public long getAsLong() {
        TimestampService timestampServiceForThisCall = timestampService;
        if (timestampServiceForThisCall == null) {
            throwNotInitializedException();
        }
        return timestampServiceForThisCall.getFreshTimestamp();
    }

    private static long throwNotInitializedException() {
        throw new NotInitializedException("The timestamp supplier is not ready yet!");
    }
}
