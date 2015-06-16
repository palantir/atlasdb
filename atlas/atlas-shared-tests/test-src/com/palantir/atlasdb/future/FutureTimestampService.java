// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.future;

import com.palantir.timestamp.DatabaseIdentifier;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class FutureTimestampService implements TimestampService {
    private final TimestampService delegate;
    private final long offset;

    public FutureTimestampService(TimestampService delegate, long offset) {
        this.delegate = delegate;
        this.offset = offset;
    }

    @Override
    public long getFreshTimestamp() {
        return delegate.getFreshTimestamp() + offset;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRunningAgainstExpectedDatabase(DatabaseIdentifier id) {
        return delegate.isRunningAgainstExpectedDatabase(id);
    }

    @Override
    public boolean isTimestampStoreStillValid() {
        return delegate.isTimestampStoreStillValid();
    }
}
