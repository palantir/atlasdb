/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.migration;

import javax.ws.rs.QueryParam;

import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampAdminService;
import com.palantir.timestamp.TimestampInvalidator;

public class IntrinsicTimestampAdminService implements TimestampAdminService {
    private final PersistentTimestampService persistentTimestampService;
    private final TimestampInvalidator timestampInvalidator;

    public IntrinsicTimestampAdminService(PersistentTimestampService persistentTimestampService,
            TimestampInvalidator timestampInvalidator) {
        this.persistentTimestampService = persistentTimestampService;
        this.timestampInvalidator = timestampInvalidator;
    }

    @Override
    public long upperBoundTimestamp() {
        return persistentTimestampService.getUpperLimitTimestampToHandOutInclusive();
    }

    @Override
    public void fastForwardTimestamp(@QueryParam("newMinimum") long newMinimumTimestamp) {
        persistentTimestampService.fastForwardTimestamp(newMinimumTimestamp);
    }

    @Override
    public void invalidateTimestamps() {
        timestampInvalidator.invalidateTimestampTable();
    }
}
