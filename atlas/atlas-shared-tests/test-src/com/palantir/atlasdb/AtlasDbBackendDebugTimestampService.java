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

package com.palantir.atlasdb;

import com.palantir.timestamp.DatabaseIdentifier;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

class AtlasDbBackendDebugTimestampService implements TimestampService {
    @Override
    public long getFreshTimestamp() {
        return Long.MAX_VALUE;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRunningAgainstExpectedDatabase(DatabaseIdentifier id) {
        return false;
    }

    @Override
    public boolean isTimestampStoreStillValid() {
        return true;
    }
}
